import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os


class ChunkedParquetWriter:
    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        self._writer: pq.ParquetWriter | None = None
        if not resume and os.path.exists(path):
            os.remove(path)

    def write(self, df: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, table.schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None


class ChunkedCSVWriter:
    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        if not resume:
            open(self.path, "w").close()
            self._header_written = False
        else:
            self._header_written = os.path.exists(path)

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path, mode="a", index=False, header=not self._header_written)
        self._header_written = True

    def close(self) -> None:
        pass


class ChunkedJSONWriter:
    def __init__(self, path: str, resume: bool = False) -> None:
        self.path = path
        self._need_comma = False
        if resume and os.path.exists(path):
            with open(self.path, "rb+") as f:
                f.seek(0, 2)
                size = f.tell()
                for back in range(1, min(20, size)):
                    f.seek(-back, 2)
                    if f.read(1) == b"]":
                        f.seek(-back, 2)
                        f.truncate()
                        break
            self._need_comma = True
        else:
            with open(self.path, "w", encoding="utf-8") as f:
                f.write("[\n")

    def write(self, df: pd.DataFrame) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            for rec in df.to_dict(orient="records"):
                if self._need_comma:
                    f.write(",\n")
                f.write(json.dumps(rec, default=str))
                self._need_comma = True

    def close(self) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write("\n]\n")


def make_writer(fmt: str, path: str, resume: bool):
    if fmt == "parquet":
        return ChunkedParquetWriter(path, resume=resume)
    elif fmt == "csv":
        return ChunkedCSVWriter(path, resume=resume)
    elif fmt == "json":
        return ChunkedJSONWriter(path, resume=resume)
    raise ValueError(f"Unknown format: {fmt}")