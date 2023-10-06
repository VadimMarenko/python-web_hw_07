from pprint import pprint

from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """Знайти 5 студентів із найбільшим середнім балом з усіх предметів."""

    pprint("*" * 50)
    pprint(f"1 -- {select_1.__doc__}")

    result = (
        session.query(
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(5)
        .all()
    )
    return result


def select_2(discipline_id: int):
    """Знайти студента із найвищим середнім балом з певного предмета."""

    pprint("*" * 50)
    pprint(f"2 -- {select_2.__doc__}")

    result = (
        session.query(
            Discipline.name,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Discipline.id == discipline_id)
        .group_by(Student.id, Discipline.name)
        .order_by(desc("average_grade"))
        .limit(1)
        .all()
    )
    return result


def select_3(discipline_id: int):
    """Знайти середній бал у групах з певного предмета."""

    pprint("*" * 50)
    pprint(f"3 -- {select_3.__doc__}")

    result = (
        session.query(
            Discipline.name,
            Group.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .join(Group)
        .filter(Discipline.id == discipline_id)
        .group_by(Group.id, Discipline.name)
        .order_by(desc("average_grade"))
        .all()
    )
    return result


def select_4():
    """Знайти середній бал на потоці (по всій таблиці оцінок)."""

    pprint("*" * 50)
    pprint(f"4 -- {select_4.__doc__}")

    result = (
        session.query(
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .all()
    )
    return result


def select_5(teacher_id: int):
    """Знайти які курси читає певний викладач."""

    pprint("*" * 50)
    pprint(f"5 -- {select_5.__doc__}")

    result = (
        session.query(
            Teacher.fullname,
            Discipline.name,
        )
        .select_from(Teacher)
        .join(Discipline)
        .filter(Teacher.id == teacher_id)
        .all()
    )
    return result


def select_6(group_id: int):
    """Знайти список студентів у певній групі."""
    """
    SELECT sg.name, s.fullname 
    FROM study_groups sg   
    JOIN students s  ON sg.id = s.group_id  
    WHERE sg.id = 1;
    """

    pprint("*" * 50)
    pprint(f"6 -- {select_6.__doc__}")

    result = (
        session.query(
            Group.name,
            Student.fullname,
        )
        .select_from(Group)
        .join(Student)
        .filter(Group.id == group_id)
        .all()
    )
    return result


def select_7(group_id: int, discipline_id: int):
    """Знайти оцінки студентів у окремій групі з певного предмета."""

    pprint("*" * 50)
    pprint(f"7 -- {select_7.__doc__}")

    result = (
        session.query(
            Group.name,
            Discipline.name,
            Student.fullname,
            Grade.grade,
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(and_(Group.id == group_id, Discipline.id == discipline_id))
        .order_by(Student.fullname)
        .all()
    )
    return result


def select_8(teacher_id: int):
    """Знайти середній бал, який ставить певний викладач зі своїх предметів."""

    pprint("*" * 50)
    pprint(f"8 -- {select_8.__doc__}")

    result = (
        session.query(
            Teacher.fullname,
            Discipline.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Discipline)
        .join(Teacher)
        .filter(Teacher.id == teacher_id)
        .group_by(Teacher.fullname, Discipline.name)
        .order_by(desc("average_grade"))
        .all()
    )
    return result


def select_9(student_id: int):
    """Знайти список курсів, які відвідує студент."""

    pprint("*" * 50)
    pprint(f"9 -- {select_9.__doc__}")

    result = (
        session.query(
            Student.fullname,
            Discipline.name,
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Student.id == student_id)
        .group_by(Student.fullname, Discipline.name)
        .order_by(Discipline.name)
        .all()
    )
    return result


def select_10(teacher_id: int, student_id: int):
    """Список курсів, які певному студенту читає певний викладач."""

    pprint("*" * 50)
    pprint(f"10 -- {select_10.__doc__}")

    result = (
        session.query(
            Teacher.fullname,
            Student.fullname,
            Discipline.name,
        )
        .select_from(Grade)
        .join(Discipline)
        .join(Student)
        .join(Teacher)
        .filter(and_(Teacher.id == teacher_id, Student.id == student_id))
        .group_by(Discipline.id, Teacher.fullname, Student.fullname)
        .order_by(Discipline.name)
        .all()
    )
    return result


def select_11(teacher_id: int, student_id: int):
    """Середній бал, який певний викладач ставить певному студентові."""

    pprint("*" * 50)
    pprint(f"11 -- {select_11.__doc__}")

    result = (
        session.query(
            Teacher.fullname,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .join(Discipline, Discipline.teacher_id == Teacher.id)
        .join(Grade)
        .join(Student)
        .filter(and_(Teacher.id == teacher_id, Student.id == student_id))
        .group_by(Teacher.fullname, Student.fullname)
        .order_by(Teacher.fullname, Student.fullname)
        .all()
    )
    return result


def select_12(group_id, discipline_id, date="2023-06-30"):
    """Оцінки студентів у певній групі з певного предмета на останньому занятті."""

    pprint("*" * 50)
    pprint(f"12 -- {select_12.__doc__}")

    # subquery = (
    #     session.query(func.MAX(Grade.date_of))
    #     .select_from(Grade)
    #     .join(Student)
    #     .filter(
    #         and_(Student.group_id == group_id, Grade.discipline_id == discipline_id)
    #     )
    # ).scalar_subquery()

    result = (
        session.query(
            Group.name,
            Discipline.name,
            Student.fullname,
            Grade.grade,
        )
        .select_from(Grade)
        .join(Discipline)
        .join(Student)
        .join(Group)
        .filter(
            and_(
                Group.id == group_id,
                Discipline.id == discipline_id,
                Grade.date_of == date,
                # Grade.date_of == subquery,
            )
        )
        .all()
    )
    return result


if __name__ == "__main__":
    pprint(select_1())
    pprint(select_2(1))
    pprint(select_3(1))
    pprint(select_4())
    pprint(select_5(3))
    pprint(select_6(1))
    pprint(select_7(1, 1))
    pprint(select_8(4))
    pprint(select_9(4))
    pprint(select_10(3, 3))
    pprint(select_11(3, 23))
    pprint(select_12(1, 5))
