import requests
import json
import pandas as pd

from websocket import create_connection
from datetime import datetime, timedelta

email = "myemail@email.com"  # Your e-mail
password = "mypassword"  # Your password
stake = 1  # Stake in the currency of your account!


class AuthClass:
    def __init__(self, emaill, passw):
        self.emaill = emaill
        self.passw = passw

        self.cookies = ""
        self.UserID = ""
        self.AuthToken = ""
        self.jwtToken = ""

        self.login()
        self.user_token()

    def login(self):
        self.cookies = ""

        url = "https://www.betpat.com/api/v1/auth?lang=en"

        payload = "{\"login\":\""+self.emaill+"\",\"password\":\""+self.passw+"\"}"
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
            'Content-Type': 'application/json;charset=UTF-8'}

        response = requests.request("PUT", url, headers=headers, data=payload, allow_redirects=False)

        if '"loggedIn":"1"' in response.text:
            print("Successful login!")

        for cookz in response.cookies:
            cookie = (str(cookz).split(" ")[1]+";")
            self.cookies = self.cookies + cookie

    def user_token(self):
        self.UserID = ""
        self.AuthToken = ""

        url = "https://www.betpat.com/api/v1/games?demo=0&gameId=none&lang=&launchCode=sportsbookNEW&merchantId=958"
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36',
            'Cookie': f'{self.cookies}'
        }
        response = requests.request("GET", url, headers=headers)
        resp_jsn = json.loads(response.text)
        self.UserID = resp_jsn["data"]["config"]["UserId"]
        self.AuthToken = resp_jsn["data"]["config"]["AuthToken"]


class ScraperClass:
    def __init__(self, authobj, sport_id):
        self.UserID = authobj.UserID
        self.AuthToken = authobj.AuthToken
        self.sport_id = str(sport_id)
        self.jwtToken = ""

        self.match_list = []
        self.time_list = []
        self.side_1 = []
        self.side_2 = []
        self.grouped_match = []

        self.match_id = []
        self.start_time = []
        self.home_player = []
        self.away_player = []
        self.home_odds = []
        self.away_odds = []
        self.home_odds_id = []
        self.away_odds_id = []
        self.corr_start_time = []

        self.home_player_org = []
        self.away_player_org = []

        self.ws = None
        self.dataset = ""
        self.dataframe = None

        self.create_connection()

    def scraper_cycle(self):
        self.scrape_matches()
        self.group_matches(self.match_list)
        self.append_data(self.grouped_match)
        self.parse_time()
        self.build_dataframe()

    def create_connection(self):
        message = '{"command":"start","rid":1,"params":{"site_id":"154","language":"en","user_id":"' + self.UserID + '","auth_token":"' + self.AuthToken + '","minify":true,"parents":["https://www.betpat.com"],"referrer":"https://www.betpat.com/","host":"https://sportsbet.esportings.com"}}'
        ws = create_connection("wss://api.sportsbet.esportings.com/socket.io?v=3")
        ws.send(message)
        response = ws.recv()
        self.jwtToken = json.loads(response)["data"]["session"]["jwtToken"]
        self.ws = ws

    def scrape_matches(self):
        today = datetime.today().strftime("%Y-%m-%d")
        tomorr = (datetime.strptime(today, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        self.match_list.clear()
        self.time_list.clear()

        get_games = '{"command":"GetGames","rid":9,"params":{"what":{"sports":["id","external_id","name","sort","alias"],"categories":["id","sport_id","name","alias","sort","broadcasts"],"tournaments":["id","sport_id","category_id","name","alias","sort","sport_id","broadcasts"],"games":["id","sport_id","category_id","tournament_id","external_id","fast_markets_count","hosted_statistics","markets_count","start_time","team_home_name","team_home_id","team_home_external_id","team_away_external_id","team_away_name","team_away_id","status","info","updated_at","sort","broadcasts","period_scores"],"markets":["id","game_id","external_id","specifiers","status","name","updated_at"],"outcomes":["id","external_id","market_id","odds","active","name","updated_at"]},"where":{"games":{"status":["not_started"],"sport_id":'+self.sport_id+',"start_time":{"gte":"'+today+'T00:00:00.000+01:00","lt":"'+tomorr+'T00:00:00.000+01:00"},"@limit":{"rows":500},"@order":{"sort":false,"tournament_id":true}},"markets":{"external_id":[1, 186],"status":["active"]},"tournaments":{"@order":{"sort":false,"id":true}}},"subscribe":true,"refetch":0,"source":"betting"}}'

        self.ws.send(get_games)

        dataset = json.loads(self.ws.recv())
        for a in dataset["data"]["mdata"][5]:
            self.match_list.append(a)

        for tim in dataset["data"]["mdata"][3]:
            self.time_list.append(tim)

        self.dataset = dataset

    def group_matches(self, match_l):
        counter_f = 0
        counter_s = counter_f + 1
        loop_until = int(len(match_l))
        self.side_1 = []
        self.side_2 = []
        self.grouped_match = []

        while counter_f < loop_until:
            if match_l[counter_f][2] == match_l[counter_s][2]:
                self.side_1.append(match_l[counter_f])
                self.side_2.append(match_l[counter_s])
            counter_f = counter_f + 2
            counter_s = counter_f + 1

        counter_f = 0
        while counter_f < len(self.side_1):
            grouped = self.side_1[counter_f] + self.side_2[counter_f]
            self.grouped_match.append(grouped)
            counter_f += 1

    def append_data(self, grouped_list):
        self.start_time.clear()
        self.match_id.clear()

        self.home_odds.clear()
        self.home_odds_id.clear()
        self.home_player.clear()
        self.home_player_org.clear()

        self.away_odds.clear()
        self.away_odds_id.clear()
        self.away_player.clear()
        self.away_player_org.clear()

        for datas in grouped_list:
            odds_id_1 = str(datas[0])
            side_id_1 = str(datas[1])
            match_id = str(datas[2])
            odds_1 = float(datas[3])
            player_1 = datas[5]
            try:
                player_1 = player_1.split(", ")[1] + " " + player_1.split(", ")[0]
            except:
                player_1 = player_1
            player_1_org = datas[5]
            match_time = str(datas[6])
            odds_id_2 = str(datas[7])
            side_id_2 = str(datas[8])
            odds_2 = float(datas[10])
            player_2 = str(datas[12])
            try:
                player_2 = player_2.split(", ")[1] + " " + player_2.split(", ")[0]
            except:
                player_2 = player_2

            player_2_org = str(datas[12])

            self.start_time.append(str(match_time))
            self.match_id.append(match_id)

            if side_id_1 == '5' and side_id_2 == '4':
                self.away_odds.append(odds_1)
                self.away_player.append(player_1)
                self.away_player_org.append(player_1_org)
                self.away_odds_id.append(odds_id_1)

                self.home_odds.append(odds_2)
                self.home_player.append(player_2)
                self.home_player_org.append(player_2_org)
                self.home_odds_id.append(odds_id_2)

            elif side_id_1 == '4' and side_id_2 == '5':
                self.away_odds.append(odds_2)
                self.away_player.append(player_2)
                self.away_player_org.append(player_2_org)
                self.away_odds_id.append(odds_id_2)

                self.home_odds.append(odds_1)
                self.home_player.append(player_1)
                self.home_player_org.append(player_1_org)
                self.home_odds_id.append(odds_id_1)

    def parse_time(self):

        for tim in self.time_list:
            found_it = 0
            counter = 0
            indexx = 0
            while counter < len(self.home_player_org):
                if self.home_player_org[counter] == tim[9]:
                    if self.away_player_org[counter] == tim[13]:
                        timy = tim[8].split("T", 1)[1][0:5]
                        timy = (datetime.strptime(timy, "%H:%M") + timedelta(hours=1)).strftime("%H:%M")  # HUN TIMEZONE
                        self.start_time[indexx] = timy
                        counter += 1
                        break
                if found_it == 0:
                    counter += 1
                    indexx += 1

        remove_index = []
        for index, val in enumerate(self.start_time):
            if ":" not in val:
                remove_index.append(index)

        for index in remove_index[::-1]:
            self.start_time.pop(index)
            self.home_player.pop(index)
            self.away_player.pop(index)
            self.home_odds.pop(index)
            self.away_odds.pop(index)
            self.home_odds_id.pop(index)
            self.away_odds_id.pop(index)

    def build_dataframe(self):
        self.dataframe = None
        dataset = {
            'TIME': self.start_time,
            'PLAYER1': self.home_player,
            'PLAYER2': self.away_player,
            'ODDS_HOME': self.home_odds,
            'ODDS_AWAY': self.away_odds,
            'ODDS_HOME_ID': self.home_odds_id,
            'ODDS_AWAY_ID': self.away_odds_id}

        database = pd.DataFrame(dataset,
                                columns=['TIME', 'PLAYER1', 'PLAYER2', 'ODDS_HOME',
                                         'ODDS_AWAY', 'ODDS_HOME_ID', 'ODDS_AWAY_ID'])
        #  This below is the hour filter, if you want to scrape only the nearest matches. Commented out, disabled.
        #  database = database.drop(database[database.TIME > (ScraperClass.hour_filter(2))].index)
        database = database.drop(database[database.TIME <= (datetime.today().strftime('%H:%M'))].index)
        database.drop_duplicates(subset=["PLAYER1", "PLAYER2"], keep=False, inplace=True)
        database.sort_values(by=["TIME"], inplace=True, ignore_index=True)
        self.dataframe = database

    @staticmethod
    def hour_filter(hourss):
        max_hour = datetime.today() + timedelta(hours=hourss)
        max_hour = max_hour.strftime('%H:%M')

        if max_hour[0:2] == "00":
            max_hour = "23:59"

        return max_hour

    def ml_bet(self, side, stakke, m_time, home_p, away_p):
        bet_found = 0
        side = side.upper()
        stakke = str(stakke)
        for index, row in self.dataframe.iterrows():
            if m_time == row["TIME"]:
                if home_p == row["PLAYER1"]:
                    if away_p == row["PLAYER2"]:
                        home_odds = row["ODDS_HOME"]
                        away_odds = row["ODDS_AWAY"]
                        home_odds_id = row["ODDS_HOME_ID"]
                        away_odds_id = row["ODDS_AWAY_ID"]
                        bet_found = 1
                        break
        if bet_found == 1:
            if side == "HOME":
                event_id = str(home_odds_id)
                price = str(home_odds)
            if side == "AWAY":
                event_id = str(away_odds_id)
                price = str(away_odds)

            bet_message = '{"command":"bet","rid":14,"params":{"bets":[{"event_id":'+event_id+',"price":'+price+'}],"amount":'+stakke+',"type":1,"mode":0,"bonus_id":0,"free_bet_count":0,"sys_bet":0,"maxOdds":null}}'

            self.ws.send(bet_message)
            conf = self.ws.recv()
            if '"status":"accepted"' in str(conf):
                print("Bet successful!")
                return True
            else:
                print("Bet unsuccessful!")
                return False
        else:
            print("Match could not be found in the database!")
            return False

########### THESE ARE FOR SHOWCASING THE USE OF THE API ONLY ###########
########### FEEL FREE TO REMOVE IT, PLAY AROUND WITH THE IDS OR CUSTOMIZE IT ###########

# SPORT IDs for API calls
table_tennis_ID = '154'
volleyball_ID = '4'

# Creating our authentication object, logging in, and getting the tokens...
betpat_user = AuthClass(email, password)

# Creating the scraper objects...
table_tennis = ScraperClass(betpat_user, table_tennis_ID)
volleyball = ScraperClass(betpat_user, volleyball_ID)


# Let's scrape the table tennis and volleyball odds!
table_tennis.scraper_cycle()
volleyball.scraper_cycle()

# Print out the results!
print(table_tennis.dataframe)
print(volleyball.dataframe)


pass
