"""Transcribe oral argument audio files stored in S3.

Downloads audio from S3, transcribes with steno (whisper or parakeet),
and uploads text, word-timestamp, and SRT outputs back to S3.
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect_aws.s3 import S3Bucket

import steno
from steno import OutputFormat, Transcript


@task(log_prints=True, task_run_name="download-{s3_key}")
async def download_audio(s3_key: str, s3_bucket_block: str) -> Path:
    """Download an audio file from S3 to a local temp directory."""
    logger = get_run_logger()

    bucket = await S3Bucket.aload(s3_bucket_block)
    suffix = Path(s3_key).suffix or ".mp3"
    tmp = Path(tempfile.mkdtemp()) / f"audio{suffix}"

    await bucket.adownload_object_to_path(s3_key, str(tmp))
    logger.info("Downloaded s3://%s/%s → %s", bucket.bucket_name, s3_key, tmp)
    return tmp


@task(log_prints=True, task_run_name="transcribe-{audio_path.stem}")
def transcribe_audio(
    audio_path: Path,
    backend: str = "whisper",
    model: str | None = None,
) -> Transcript:
    """Transcribe an audio file using steno."""
    logger = get_run_logger()
    logger.info("Transcribing %s (backend=%s, model=%s)", audio_path, backend, model)

    transcript = steno.transcribe(
        audio_path,
        backend=backend,
        model=model,
        output=OutputFormat.SRT,
    )

    logger.info(
        "Transcribed %.1fs of audio → %d words",
        transcript.duration,
        len(transcript.words),
    )
    return transcript


@task(log_prints=True, task_run_name="upload-transcripts-{s3_key_stem}")
async def upload_transcripts(
    transcript: Transcript,
    s3_key_stem: str,
    s3_bucket_block: str,
) -> dict[str, str]:
    """Upload transcript outputs (txt, words tsv, srt) to S3.

    Returns a dict mapping format name to S3 URI.
    """
    logger = get_run_logger()
    bucket = await S3Bucket.aload(s3_bucket_block)
    bucket_name = bucket.bucket_name

    uploads = {}

    # Plain text
    txt_key = f"{s3_key_stem}.txt"
    await bucket.aupload_from_file_object(
        io.BytesIO(transcript.text.encode()),
        txt_key,
    )
    uploads["text"] = f"s3://{bucket_name}/{txt_key}"

    # Word-level timestamps as TSV
    tsv_key = f"{s3_key_stem}.words.tsv"
    lines = ["start\tend\tword"]
    for w in transcript.words:
        lines.append(f"{w.start:.3f}\t{w.end:.3f}\t{w.text}")
    await bucket.aupload_from_file_object(
        io.BytesIO("\n".join(lines).encode()),
        tsv_key,
    )
    uploads["words"] = f"s3://{bucket_name}/{tsv_key}"

    # SRT subtitles
    srt_key = f"{s3_key_stem}.srt"
    await bucket.aupload_from_file_object(
        io.BytesIO(transcript.srt.encode()),
        srt_key,
    )
    uploads["srt"] = f"s3://{bucket_name}/{srt_key}"

    for fmt, uri in uploads.items():
        logger.info("Uploaded %s → %s", fmt, uri)

    return uploads


@flow(name="transcribe-oral-argument", log_prints=True)
async def transcribe_oral_argument(
    s3_audio_key: str,
    s3_bucket_block: str = "scrapers",
    backend: str = "whisper",
    model: str | None = None,
) -> dict[str, str]:
    """Transcribe a single oral argument audio file from S3.

    Args:
        s3_audio_key: S3 object key for the audio file.
        s3_bucket_block: Name of the Prefect S3Bucket block.
        backend: Steno backend — "whisper" or "parakeet".
        model: Model name/size. Defaults per backend.

    Returns:
        Dict mapping format name to S3 URI of uploaded transcript.
    """
    audio_path = await download_audio(s3_audio_key, s3_bucket_block)

    transcript = transcribe_audio(audio_path, backend=backend, model=model)

    # Derive output key stem from input key (strip audio extension)
    stem = str(Path(s3_audio_key).with_suffix(""))
    uploads = await upload_transcripts(transcript, stem, s3_bucket_block)

    # Clean up temp file
    audio_path.unlink(missing_ok=True)

    return uploads
