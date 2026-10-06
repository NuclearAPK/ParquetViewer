"""
Parquet Viewer - FastAPI backend (v2)

Supports:
- Opening directories that contain Parquet datasets (with iceberg.schema.json,
  _common_metadata and *.parquet files)
- Uploading single Parquet files
- Schema view (Iceberg fields: id, name, type, required, doc + Arrow types)
- Data view with pagination, search, CSV/JSON export
- Proper serialization of UUID (bytes), Timestamp, Decimal, numpy scalars
"""

import io
import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="Parquet Viewer", version="2.0.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# In-memory registry of opened datasets
datasets: Dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def conv_value(v: Any) -> Any:
    """Convert a single cell value into something JSON/CSV friendly."""
    if v is None:
        return None
    if isinstance(v, bytes):
        if len(v) == 16:
            try:
                return str(uuid.UUID(bytes=v))
            except Exception:
                pass
        return v.hex()
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, float) and v != v:
        return None
    return v


def df_to_records(df: pd.DataFrame) -> List[dict]:
    cols = list(df.columns)
    return [
        {c: conv_value(v) for c, v in zip(cols, row)}
        for row in df.itertuples(index=False, name=None)
    ]


def to_searchable(s: pd.Series) -> pd.Series:
    """Stringify a column so search can also match UUID hex / decimals."""
    if s.dtype == object:
        def conv(v):
            if isinstance(v, bytes):
                if len(v) == 16:
                    try:
                        return str(uuid.UUID(bytes=v))
                    except Exception:
                        pass
                return v.hex()
            return str(v)
        return s.map(conv)
    if isinstance(s.dtype, pd.ArrowDtype) or str(s.dtype) in ("str", "string"):
        return s.astype(str)
    return s.astype(str)


def is_string_col(s: pd.Series) -> bool:
    return s.dtype == object or pd.api.types.is_string_dtype(s)


# ---------------------------------------------------------------------------
# Dataset opening
# ---------------------------------------------------------------------------

def load_iceberg_schema(base: Path) -> Optional[dict]:
    """Read iceberg.schema.json (UTF-8 with cp1251 fallback)."""
    p = base / "iceberg.schema.json"
    if not p.exists():
        return None
    raw = p.read_bytes()
    for enc in ("utf-8-sig", "cp1251"):
        try:
            return json.loads(raw.decode(enc))
        except Exception:
            continue
    return None


def arrow_schema_of(f: Path) -> List[dict]:
    schema = pq.read_schema(f)
    return [
        {"name": fld.name, "type": str(fld.type), "nullable": fld.nullable}
        for fld in schema
    ]


def open_path(path_str: str) -> dict:
    """Open a directory (dataset) or a single .parquet file."""
    p = Path(path_str).expanduser()
    if not str(p).strip():
        raise HTTPException(status_code=400, detail="Path is empty")
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {p}")

    if p.is_file():
        if p.suffix.lower() != ".parquet":
            raise HTTPException(status_code=400, detail="Only .parquet files can be opened")
        base, files, kind = p.parent, [p], "file"
    else:
        files = sorted(p.glob("*.parquet"))
        if not files:
            raise HTTPException(status_code=400, detail=f"No .parquet files in {p}")
        base, kind = p, "directory"

    iceberg = load_iceberg_schema(base)
    arrow_schema = arrow_schema_of(files[0])

    # Row counts (metadata read is cheap)
    row_counts: Dict[str, int] = {}
    total = 0
    for f in files:
        try:
            n = pq.read_metadata(f).num_rows
        except Exception:
            n = -1
        row_counts[f.name] = n
        if n > 0:
            total += n

    has_common = (base / "_common_metadata").exists()

    ds_id = uuid.uuid4().hex[:8]
    ds = {
        "id": ds_id,
        "name": base.name if kind == "directory" else p.name,
        "kind": kind,
        "path": str((p if kind == "directory" else p).resolve()),
        "base": str(base.resolve()),
        "files": [f.name for f in files],
        "file_paths": {f.name: str(f.resolve()) for f in files},
        "row_counts": row_counts,
        "total_rows": total,
        "arrow_schema": arrow_schema,
        "iceberg": iceberg,
        "has_common_metadata": has_common,
        "_dfs": {},
    }
    datasets[ds_id] = ds
    return ds


def get_ds(ds_id: str) -> dict:
    ds = datasets.get(ds_id)
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return ds


def get_df(ds: dict, fname: Optional[str] = None) -> tuple:
    fname = fname or ds["files"][0]
    if fname not in ds["file_paths"]:
        raise HTTPException(status_code=404, detail=f"File not found in dataset: {fname}")
    if fname not in ds["_dfs"]:
        ds["_dfs"][fname] = pd.read_parquet(ds["file_paths"][fname])
    return ds["_dfs"][fname], fname


def schema_payload(ds: dict) -> dict:
    arrow = ds["arrow_schema"]
    amap = {f["name"]: f for f in arrow}
    fields: List[dict] = []
    iceberg = ds.get("iceberg")
    comment = None
    schema_id = None

    if iceberg and iceberg.get("schema", {}).get("fields"):
        comment = iceberg.get("tableComment")
        schema_id = iceberg["schema"].get("schema-id")
        for fl in iceberg["schema"]["fields"]:
            a = amap.get(fl["name"], {})
            fields.append({
                "id": fl.get("id"),
                "name": fl["name"],
                "iceberg_type": fl.get("type"),
                "arrow_type": a.get("type"),
                "required": fl.get("required"),
                "nullable": a.get("nullable"),
                "doc": fl.get("doc", ""),
            })
    else:
        for i, f in enumerate(arrow, 1):
            fields.append({
                "id": i,
                "name": f["name"],
                "iceberg_type": None,
                "arrow_type": f["type"],
                "required": not f["nullable"],
                "nullable": f["nullable"],
                "doc": "",
            })

    return {
        "dataset_id": ds["id"],
        "name": ds["name"],
        "path": ds["path"],
        "kind": ds["kind"],
        "table_comment": comment,
        "schema_id": schema_id,
        "has_iceberg_schema": bool(iceberg),
        "has_common_metadata": ds["has_common_metadata"],
        "fields": fields,
        "arrow_schema": arrow,
        "files": [
            {"name": f, "rows": ds["row_counts"].get(f, -1)}
            for f in ds["files"]
        ],
        "total_rows": ds["total_rows"],
    }


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------

class OpenReq(BaseModel):
    path: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {"message": "Parquet Viewer API v2.0", "version": "2.0.0"}


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "2.0.0",
        "datasets": len(datasets),
    }


@app.get("/fs")
def fs_list(path: str = Query(".")):
    """Browse the server filesystem to find Parquet datasets."""
    p = Path(path).expanduser()
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {p}")
    if p.is_file():
        p = p.parent
    p = p.resolve()

    dirs, files = [], []
    try:
        for c in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if c.is_dir():
                if not c.name.startswith("."):
                    dirs.append(c.name)
            elif c.suffix.lower() == ".parquet":
                files.append(c.name)
    except PermissionError:
        raise HTTPException(status_code=403, detail=f"Access denied: {p}")

    return {
        "path": str(p),
        "parent": str(p.parent) if p.parent != p else None,
        "dirs": dirs,
        "parquet_files": files,
        "has_iceberg_schema": (p / "iceberg.schema.json").exists(),
        "has_common_metadata": (p / "_common_metadata").exists(),
    }


@app.post("/open")
def open_dataset(req: OpenReq):
    """Open a directory (Parquet dataset) or a single .parquet file by path."""
    ds = open_path(req.path)
    return schema_payload(ds)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload a Parquet file; it becomes an open dataset."""
    if not (file.filename or "").lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="Only .parquet files are allowed")

    dest = UPLOAD_DIR / Path(file.filename).name
    dest.write_bytes(await file.read())

    ds = open_path(str(dest))
    return {
        "message": f"File '{file.filename}' uploaded successfully",
        "dataset": schema_payload(ds),
    }


@app.get("/ds")
def list_datasets():
    return {
        "datasets": [
            {
                "id": d["id"],
                "name": d["name"],
                "kind": d["kind"],
                "path": d["path"],
                "files": len(d["files"]),
                "total_rows": d["total_rows"],
                "columns": len(d["arrow_schema"]),
                "has_iceberg_schema": bool(d.get("iceberg")),
            }
            for d in datasets.values()
        ]
    }


@app.get("/ds/{ds_id}")
def get_dataset(ds_id: str):
    return schema_payload(get_ds(ds_id))


@app.get("/ds/{ds_id}/data")
def get_data(
    ds_id: str,
    file: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=50000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None),
):
    ds = get_ds(ds_id)
    df, fname = get_df(ds, file)

    total = len(df)
    if search:
        mask = pd.Series(False, index=df.index)
        for col in df.columns:
            if is_string_col(df[col]):
                mask = mask | to_searchable(df[col]).str.contains(
                    search, na=False, case=False, regex=False
                )
        view = df[mask]
    else:
        view = df

    page = view.iloc[offset:offset + limit]
    dtypes = {c: str(t) for c, t in zip(df.columns, df.dtypes)}

    return {
        "dataset_id": ds_id,
        "file": fname,
        "columns": list(df.columns),
        "dtypes": dtypes,
        "data": df_to_records(page),
        "row_count": len(view),
        "file_row_count": total,
        "total_rows_returned": len(page),
        "offset": offset,
        "limit": limit,
    }


@app.get("/ds/{ds_id}/export")
def export_data(
    ds_id: str,
    format: str = Query("csv"),
    file: Optional[str] = Query(None),
):
    ds = get_ds(ds_id)
    df, fname = get_df(ds, file)

    if format == "csv":
        out = df.map(conv_value) if hasattr(df, "map") else df.apply(
            lambda c: c.map(conv_value)
        )
        return {
            "filename": Path(fname).stem,
            "format": "csv",
            "content": out.to_csv(index=False),
            "row_count": len(df),
        }
    elif format == "json":
        return {
            "filename": Path(fname).stem,
            "format": "json",
            "content": json.dumps(df_to_records(df), ensure_ascii=False),
            "row_count": len(df),
        }
    raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")


# ---------------------------------------------------------------------------
# Legacy endpoints (kept for backward compatibility)
# ---------------------------------------------------------------------------

@app.get("/files")
async def list_files():
    return {"files": [f.name for f in UPLOAD_DIR.glob("*.parquet")]}
