from typing import Any
import requests
import json
import psycopg2


def get_hh_data(employers_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о работодателях и вакансиях с помощью API HeadHunter."""
    employers_data = []
    for employer_id in employers_ids:
        hh_employer_response = requests.get(f'https://api.hh.ru/employers/{employer_id}')
        hh_employer_data = hh_employer_response.content.decode()
        employer = json.loads(hh_employer_data)
        #print(employer['name'])
        employer_vacancies_data = []
        pages = 1
        page = 0
        while page <= pages:
            params = {
                'employer_id': employer_id,
                'page': page,
            }
            # vacancies_response = requests.get(f'{item["vacancies_url"]}')
            hh_employer_vacancies_response = requests.get('https://api.hh.ru/vacancies', params)
            hh_emp_vac_data = hh_employer_vacancies_response.content.decode()
            employer_vacancies = json.loads(hh_emp_vac_data)
            # print(employer_vacancies)
            employer_vacancies_data.extend(employer_vacancies['items'])
            pages = employer_vacancies['pages']
            page += 1
        employers_data.append({
            "employer": employer,
            "vacancies": employer_vacancies_data
        }
        )
    # print(employers_data[0])
    return employers_data


def create_database(database_name: str, params: dict) -> None:
    """ Создание базы данных и таблиц для сохранения данных о работодателях и вакансиях."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    employer_url VARCHAR(255),
                    employer_hh_url VARCHAR(255),
                    city VARCHAR(100),
                    open_vacancies INTEGER,
                    description TEXT                    
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    employer_id INT REFERENCES employers(employer_id),
                    title VARCHAR NOT NULL,
                    city VARCHAR,
                    salary INTEGER,
                    publish_date DATE,
                    vacancy_url TEXT,
                    requirement VARCHAR,
                    responsibility VARCHAR,
                    schedule VARCHAR,
                    experience VARCHAR,
                    employment VARCHAR
                )
            """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """ Сохранение данных о работодателях и вакансиях в базу данных."""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in data:
            employer_data = employer['employer']
            cur.execute(
                """
                INSERT INTO employers ("title", "employer_url", "employer_hh_url", "city", "open_vacancies")
                VALUES (%s, %s, %s, %s, %s)
                RETURNING employer_id
                """,
                (employer_data['name'], employer_data['site_url'], employer_data['alternate_url'], employer_data['area']['name'], employer_data['open_vacancies'])
            )
            employer_id = cur.fetchone()[0]
            # print(employer_id)
            # print("hehey")

            vacancies_data = employer['vacancies']
            for vacancy in vacancies_data:
                if vacancy['salary'] is None:
                    salary = 0
                else:
                    salary = vacancy['salary']['from']
                cur.execute(
                    """
                    INSERT INTO vacancies ("employer_id", "title", "city", "salary", "publish_date", "vacancy_url", 
                    "requirement", "responsibility", "schedule", "experience", "employment")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (employer_id, vacancy['name'], vacancy['area']['name'], salary, vacancy['published_at'],
                     vacancy['alternate_url'], vacancy['snippet']['requirement'], vacancy['snippet']['responsibility'],
                     vacancy['schedule']['name'], vacancy['experience']['name'], vacancy['employment']['name'])
                )

    conn.commit()
    conn.close()
