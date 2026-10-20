import argparse
import json
import os
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


class Database:
    def __init__(self, **conn_params):
        self._conn = psycopg.connect(**conn_params, row_factory=dict_row)

    def execute(self, sql, params=None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params)

    def executemany(self, sql, rows):
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)

    def fetch_all(self, sql, params=None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc):
        return self._conn.__exit__(*exc)


class JsonReader:
    @staticmethod
    def read(path: Path) -> list[dict]:
        with open(path, encoding="utf-8") as f:
            return json.load(f)


class DataSchema:
    def __init__(self, db: Database):
        self._db = db

    def create_schema(self):
        self._db.execute("""
            DROP TABLE IF EXISTS students;
            DROP TABLE IF EXISTS rooms;
            CREATE TABLE rooms (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
            CREATE TABLE students (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                birthday DATE,
                room INTEGER REFERENCES rooms(id),
                sex CHAR(1) CHECK (sex IN ('M', 'F'))
            );
        """)

    def apply_indexes(self, sql_path: Path):
        self._db.execute(sql_path.read_text(encoding="utf-8"))


class DataLoader:
    def __init__(self, db: Database):
        self._db = db

    def load_rooms(self, rooms: list[dict]):
        self._db.executemany(
            "INSERT INTO rooms (id, name) VALUES (%s, %s)",
            [(r["id"], r["name"]) for r in rooms],
        )

    def load_students(self, students: list[dict]):
        self._db.executemany(
            "INSERT INTO students (id, name, birthday, sex, room) VALUES (%s, %s, %s, %s, %s)",
            [(s["id"], s["name"], s["birthday"], s["sex"], s["room"]) for s in students],
        )


class Queries:
    def __init__(self, db: Database):
        self._db = db

    def student_count(self):
        return self._db.fetch_all("""
            SELECT r.id, r.name AS room_name, COUNT(s.id) AS student_count
            FROM rooms r
            LEFT JOIN students s ON r.id = s.room
            GROUP BY r.id, r.name
            ORDER BY r.id
        """)

    def min_avg_age(self):
        return self._db.fetch_all("""
            SELECT r.id, r.name AS room_name,
                   ROUND(AVG(EXTRACT(YEAR FROM AGE(s.birthday))), 2)::float AS avg_age
            FROM rooms r
            JOIN students s ON r.id = s.room
            GROUP BY r.id, r.name
            ORDER BY avg_age ASC, r.id
            LIMIT 5
        """)

    def max_age_diff(self):
        return self._db.fetch_all("""
            SELECT r.id, r.name AS room_name,
                   (MAX(EXTRACT(YEAR FROM AGE(s.birthday)))
                  - MIN(EXTRACT(YEAR FROM AGE(s.birthday))))::float AS age_difference
            FROM rooms r
            JOIN students s ON r.id = s.room
            GROUP BY r.id, r.name
            ORDER BY age_difference DESC, r.id
            LIMIT 5
        """)

    def mixed_gender(self):
        return self._db.fetch_all("""
            SELECT r.id, r.name AS room_name
            FROM rooms r
            JOIN students s ON r.id = s.room
            GROUP BY r.id, r.name
            HAVING COUNT(DISTINCT s.sex) > 1
            ORDER BY r.id
        """)


class Exporter(ABC):
    @abstractmethod
    def export(self, data: dict, path: Path) -> None: ...


class JsonExporter(Exporter):
    def export(self, data, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


class XmlExporter(Exporter):
    def export(self, data, path):
        root = ET.Element("results")
        for section, rows in data.items():
            sec_el = ET.SubElement(root, section)
            for row in rows:
                item = ET.SubElement(sec_el, "item")
                for key, value in row.items():
                    ET.SubElement(item, key).text = str(value)
        tree = ET.ElementTree(root)
        ET.indent(tree)
        tree.write(path, encoding="utf-8", xml_declaration=True)


EXPORTERS: dict[str, type[Exporter]] = {"json": JsonExporter, "xml": XmlExporter}


class ReportService:
    def __init__(self, db: Database, exporter: Exporter):
        self._db = db
        self._exporter = exporter

    def run(self, rooms_path: Path, students_path: Path, indexes_path: Path, output: Path):
        schema = DataSchema(self._db)
        loader = DataLoader(self._db)

        schema.create_schema()
        loader.load_rooms(JsonReader.read(rooms_path))
        loader.load_students(JsonReader.read(students_path))
        schema.apply_indexes(indexes_path) 

        q = Queries(self._db)
        results = {
            "room_student_count": q.student_count(),
            "min_avg_age_rooms": q.min_avg_age(),
            "max_age_diff_rooms": q.max_age_diff(),
            "mixed_gender_rooms": q.mixed_gender(),
        }
        self._exporter.export(results, output)


def parse_args():
    p = argparse.ArgumentParser(description="Отчёт по студентам и комнатам")
    p.add_argument("--students", required=True, type=Path, help="путь к students.json")
    p.add_argument("--rooms", required=True, type=Path, help="путь к rooms.json")
    p.add_argument("--format", required=True, choices=EXPORTERS.keys(), help="json или xml")
    p.add_argument("--output", type=Path, help="путь к results.json или results.xml (по умолчанию в текущей папке)")
    return p.parse_args()

def load_db_params() -> dict:
    """Читает параметры подключения к БД из переменных окружения (.env)."""
    load_dotenv()
    required = ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Не заданы переменные окружения: {', '.join(missing)}")
    return {
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "host": os.environ["DB_HOST"],
        "port": int(os.environ["DB_PORT"]),
    }

def main():
    args = parse_args()
    base = Path(__file__).parent
    output = args.output or base / f"results.{args.format}"

    with Database(**load_db_params()) as db:
        service = ReportService(db, EXPORTERS[args.format]())
        service.run(args.rooms, args.students, base / "indexes.sql", output)

    print(f"Готово: {output}")


if __name__ == "__main__":
    main()