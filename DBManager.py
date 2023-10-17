import psycopg2


class DBManager:
    """
    Класс, который подключается к базе данных 'hh'
    """
    def __init__(self, params):
        self.conn = psycopg2.connect(dbname='hh', **params)

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT employers.title, COUNT(*) FROM employers JOIN vacancies USING (employer_id) "
                                "GROUP BY employers.title;")
                    employers = cur.fetchall()
                    for employer in employers:
                        print(employer)
        finally:
            self.conn.close()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        """
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT vacancies.title, vacancies.salary, vacancies.vacancy_url, employers.title FROM "
                                "vacancies JOIN employers USING (employer_id);")
                    vacancies = cur.fetchall()
                    for vacancy in vacancies:
                        print(vacancy)
        finally:
            self.conn.close()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям
        """
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT ROUND (AVG(salary)) FROM vacancies;")
                    average_vacancy = cur.fetchone()
                    print(f"Средняя зарплата по вакансиям: {average_vacancy}")
        finally:
            self.conn.close()

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        """
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT * FROM vacancies WHERE salary > (SELECT AVG(salary) FROM VACANCIES) ORDER BY salary DESC;")
                    vacancies = cur.fetchall()
                    for vacancy in vacancies:
                        print(vacancy)
        finally:
            self.conn.close()

    def get_vacancies_with_keyword(self, word):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python
        """
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT *, employers.title FROM vacancies JOIN employers USING (employer_id) "
                                f"WHERE vacancies.title LIKE '%{word}%';")
                    vacancies = cur.fetchall()
                    if len(vacancies) == 0:
                        print("По вашему запросу ничего не найдено")
                    else:
                        for vacancy in vacancies:
                            print(vacancy)
        finally:
            self.conn.close()
