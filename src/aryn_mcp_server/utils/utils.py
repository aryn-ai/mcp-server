import os
import sys
import json
import time
import zipfile
import pandas as pd

from PIL import Image
from pathlib import Path
from functools import wraps
from typing import Dict, Union, List
from io import BytesIO, StringIO


def get_output_dir() -> Path:
    output_dir = os.environ.get("ARYN_MCP_OUTPUT_DIR")
    if output_dir:
        path = Path(output_dir).resolve()
    else:
        project_root = Path(__file__).parent.parent.parent.resolve()
        path = project_root / "temp"

    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()  # Return absolute path


def save_file(
    data: Union[Dict, str, Image.Image, bytes],
    filename: str,
    output_format: str = "json",
) -> Path:
    try:
        base_dir = get_output_dir()  # This is now an absolute path
        path = ensure_unique_filename(base_dir / filename, f".{output_format}")
        if isinstance(data, dict) or isinstance(data, list) and output_format == "json":
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        elif isinstance(data, str) and output_format == "markdown":
            with open(path, "w", encoding="utf-8") as f:
                f.write(data)
        elif isinstance(data, dict) and output_format == "markdown":
            with open(path, "w", encoding="utf-8") as f:
                f.write(data.get("markdown", ""))
        elif isinstance(data, Image.Image) and output_format in ["png", "jpg", "jpeg"]:
            data.save(path, format=output_format.upper())
        elif isinstance(data, bytes) and output_format == "zip":
            with open(path, "wb") as f:
                f.write(data)
        else:
            raise ValueError(f"Unsupported combination of data type {type(data)} and format {output_format}")

        return path.resolve()  # Return absolute path

    except Exception as e:
        raise e


def ensure_unique_filename(base_path: Union[str, Path], extension: str) -> Path:
    base_path = Path(base_path)
    counter = 1
    new_path = base_path.with_suffix(extension)

    while new_path.exists():
        new_path = base_path.parent / f"{base_path.stem}_{counter}{extension}"
        counter += 1

    return new_path


def create_zip_from_dataframes(dataframes: List[pd.DataFrame]) -> bytes:
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for i, df in enumerate(dataframes):
            if df is not None and not df.empty:
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_content = csv_buffer.getvalue()

                csv_filename = f"table_{i+1}.csv"
                zip_file.writestr(csv_filename, csv_content)

    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        print(f"{func.__name__} took {end_time - start_time:.2f} seconds", file=sys.stderr)
        return result

    return wrapper
