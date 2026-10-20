CREATE INDEX IF NOT EXISTS idx_students_room_covering
    ON students (room) INCLUDE (birthday, sex);

ANALYZE students;