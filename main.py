from config import config
from utils import get_hh_data, create_database, save_data_to_database
from DBManager import DBManager


def main():
    employers_ids = [
        5060211, # "Группа компаний Астра",
        4350075, # "ГК Innostage",
        142231, # "INFOWATCH",
        2575, #"Бэнкс Софт Системс",
        26624, #"Positive Technologies",
        2381, #"Softline",
        588914, #"Aviasales.ru",
        1102601, #"Группа Самолет",
        3646686, #"АЙФЭЛЛ",
        1885395 #"Р-Вижн"
        ]
    params = config()

    data = get_hh_data(employers_ids)
    create_database("hh", params)
    save_data_to_database(data, "hh", params)

    print("Добрый день!"
          "У нас есть база данных с вакансиями 10 работодателей. ")
    while True:
        print("Выберите запрос:")
        user_answer = input("""'1' - получить список всех компаний и количество вакансий у каждой компании;
'2' - получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию;
'3' - получить среднюю зарплату по вакансиям;
'4' - получить список всех вакансий, у которых зарплата выше средней по всем вакансиям;
'5' - получить список всех вакансий, в названии которых содержится выбранное слово,
'exit' - выйти.
""")
        if user_answer == "1":
            db_result = DBManager(params)
            db_result.get_companies_and_vacancies_count()
        elif user_answer == "2":
            db_result = DBManager(params)
            db_result.get_all_vacancies()
        elif user_answer == "3":
            db_result = DBManager(params)
            db_result.get_avg_salary()
        elif user_answer == "4":
            db_result = DBManager(params)
            db_result.get_vacancies_with_higher_salary()
        elif user_answer == "5":
            word = input("Введите слово для поиска:   ")
            db_result = DBManager(params)
            db_result.get_vacancies_with_keyword(word)
        elif user_answer.lower() == "exit":
            print("До свидания!")
            break
        else:
            continue


if __name__ == '__main__':
    main()
