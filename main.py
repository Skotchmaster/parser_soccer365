import re
import requests
from collections import defaultdict
from lxml import html
import pymysql
from config import host, user, password, db_name


def format_year(year_string):
    match = re.search(r"\b(\d{4}(?:/\d{4})?)\b", year_string)
    if match:
        year = match.group(1)
        if "/" in year:
            return year
        else:
            return year.split("/")[0]
    else:
        return year_string


def fetch_html_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def parse_competition_results(competition_index, count_of_years):
    base_url = f'https://soccer365.ru/competitions/{competition_index}/history/'
    html_content = fetch_html_content(base_url)
    tree = html.fromstring(html_content)

    all_years = defaultdict(list)
    all_results = defaultdict(list)

    for year_index in range(1, count_of_years + 1):
        year = tree.xpath(f"//div[@class='page_main_content w700']//tbody//tr[{year_index}]//a//span/text()")[0]
        if competition_index == 13:
            year = 'Российская ' + year

        year = format_year(year)
        year_url = tree.xpath(f"//div[@class='page_main_content w700']//tbody//tr[{year_index}]//a/@href")[0]
        year_html = fetch_html_content(f"https://soccer365.ru/{year_url}")
        year_tree = html.fromstring(year_html)

        team_images = year_tree.xpath('//table[@class="tablesorter stngs"]//tbody//img[1]/@src')
        standings = []
        team_index = 1

        while year_tree.xpath(f"//table[@class='tablesorter stngs']//tbody//tr[{team_index}]//td/text()"):
            team_data = [""] * 10
            team_data[0] = team_images[team_index - 1]
            team_xpath = f"//table[@class='tablesorter stngs']//tbody//tr[{team_index}]//td"
            team_data[1] = year_tree.xpath(f'{team_xpath}//span//a/text()')[0]

            for value in range(7):
                team_data[value + 2] = year_tree.xpath(f'{team_xpath}/text()')[value]

            if year_tree.xpath(f'{team_xpath}/b/text()'):
                team_data[9] = year_tree.xpath(f'{team_xpath}/b/text()')[0]
            else:
                team_data[9] = year_tree.xpath(f'{team_xpath}/span/text()')[0]

            team_index += 1
            standings.append(team_data)

        all_years[year] = standings

        results_url = year_tree.xpath('//span[@class="tabs_item"][2]//a/@href')[0]
        results_html = fetch_html_content(f"https://soccer365.ru/{results_url}")
        results_tree = html.fromstring(results_html)

        results = defaultdict(list)
        match_count = 1

        while results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]/text()'):
            tour = results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//div[@class="cmp_stg_ttl"]/text()')[0]
            games = []
            game_index = 1

            while results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//div[@class="game_block "][{game_index}]'):
                time = results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//span[contains(@class, "size")][1]/text()')
                teams = results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//div[@class="game_block "][{game_index}]//div[@class="img16"][1]/span/text()')
                teams_img = results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//div[@class="game_block "][{game_index}]//img/@src')
                score = results_tree.xpath(f'//div[@class="live_comptt_bd "][{match_count}]//div[@class="game_block "][{game_index}]//div[@class="gls"][1]/text()')

                if time[game_index - 1] not in ["Остановлен", "Отменен"]:
                    game = [time[game_index - 1], teams_img[0], teams[0], score[0], score[1], teams_img[1], teams[1]]
                    games.append(game)

                game_index += 1

            results[tour].extend(games)
            match_count += 1

        all_results[year] = results

    return all_years, all_results


def table_for_other(competition_index, count_of_years):
    url = f"https://soccer365.ru/competitions/{competition_index}/history/table[@id='history']"
    html_content = fetch_html_content(url)
    tree = html.fromstring(html_content)

    competition_table = defaultdict(list)

    for year_index in range(1, count_of_years + 1):
        year = tree.xpath(f"//tbody//tr[{year_index}]//td[1]//span/text()")[0]
        year = format_year(year)

        for team_position in range(2, 6):
            team_img = tree.xpath(f"//tbody//tr[{year_index}]//td[{team_position}]//img/@src")
            team_name = tree.xpath(f"//tbody//tr[{year_index}]//td[{team_position}]//span/a/text()")

            if team_img and team_name:
                competition_table[year].append([team_img[0], team_name[0]])
            else:
                competition_table[year].append(["", ""])

    return competition_table


def create_db_connection():
    try:
        connection = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as ex:
        print("Connection refused...")
        print(ex)
        return None


def create_table(connection, table_name, columns):
    with connection.cursor() as cursor:
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})"
        cursor.execute(create_table_query)
        connection.commit()


def insert_competition_data(connection, table_name, data):
    with connection.cursor() as cursor:
        insert_query = (
            f"INSERT INTO `{table_name}` (соревнование, первое_место, эмблема_первого_места, "
            "второе_место, эмблема_второго_места, третье_место, эмблема_третьего_места, "
            "четвертое_место, эмблема_четвертого_места) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cursor.execute(insert_query, data)
        connection.commit()


def insert_match_results(connection, table_name, year, results):
    with connection.cursor() as cursor:
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS `{table_name} {year}_результаты_туров` ("
            "id INT AUTO_INCREMENT PRIMARY KEY, "
            "тур VARCHAR(64), "
            "дата VARCHAR(64), "
            "первая_команда VARCHAR(64), "
            "эмблема_первой_команды VARCHAR(64), "
            "вторая_команда VARCHAR(32), "
            "эмблема_второй_команды VARCHAR(64), "
            "счет VARCHAR(64));"
        )
        cursor.execute(create_table_query)

        for tour in results:
            for game in results[tour]:
                insert_query = (
                    f"INSERT INTO `{table_name} {year}_результаты_туров` (тур, дата, первая_команда, "
                    "эмблема_первой_команды, вторая_команда, эмблема_второй_команды, счет) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s);"
                )
                cursor.execute(insert_query, (
                    tour, game[0], game[2], game[1], game[6], game[5], f"{game[3]} {game[4]}"
                ))
                connection.commit()


def insert_team_standings(connection, table_name, year, standings):
    with connection.cursor() as cursor:
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS `{table_name} {year}_таблица` ("
            "id INT AUTO_INCREMENT PRIMARY KEY, "
            "эмблема_команды VARCHAR(64), "
            "название_команды VARCHAR(64), "
            "игры VARCHAR(64), "
            "победы VARCHAR(64), "
            "ничьи VARCHAR(64), "
            "поражения VARCHAR(64), "
            "забитые_мячи VARCHAR(64), "
            "пропущенные_мячи VARCHAR(64), "
            "разница VARCHAR(64), "
            "очки VARCHAR(64));"
        )
        cursor.execute(create_table_query)

        for team in standings:
            insert_query = (
                f"INSERT INTO `{table_name} {year}_таблица` (эмблема_команды, название_команды, игры, "
                "победы, ничьи, поражения, забитые_мячи, пропущенные_мячи, разница, очки) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            )
            cursor.execute(insert_query, (
                team[0], team[1], team[2], team[3], team[4], team[5], team[6], team[7], team[8], team[9]
            ))
            connection.commit()


def main():
    connection = create_db_connection()
    if not connection:
        return

    try:
        competitions = [
            (19, 68, "лига чемпионов уефа"),
            (24, 17, "чемпионат европы"),
            (742, 22, "чемпионат мира"),
            (13, 32, 'российская премьер лига')
        ]

        for competition_index, count_of_years, table_name in competitions:
            if table_name in ["лига чемпионов уефа", "чемпионат европы", "чемпионат мира"]:
                competition_data = table_for_other(competition_index, count_of_years)

                create_table(connection, table_name, """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    соревнование VARCHAR(64),
                    первое_место VARCHAR(64),
                    эмблема_первого_места VARCHAR(255),
                    второе_место VARCHAR(64),
                    эмблема_второго_места VARCHAR(255),
                    третье_место VARCHAR(64),
                    эмблема_третьего_места VARCHAR(255),
                    четвертое_место VARCHAR(64),
                    эмблема_четвертого_места VARCHAR(255)
                """)

                for year, teams in competition_data.items():
                    insert_competition_data(connection, table_name, (
                        year, teams[0][1], teams[0][0], teams[1][1], teams[1][0],
                        teams[2][1], teams[2][0], teams[3][1], teams[3][0]
                    ))

                all_years, all_results = parse_competition_results(competition_index, count_of_years)
                for year in all_results:
                    insert_match_results(connection, table_name, year, all_results[year])

            else:
                all_years, all_results = parse_competition_results(competition_index, count_of_years)

                for year in all_results:
                    insert_match_results(connection, table_name, year, all_results[year])

                for year in all_years:
                    insert_team_standings(connection, table_name, year, all_years[year])

    finally:
        connection.close()


if __name__ == "__main__":
    main()