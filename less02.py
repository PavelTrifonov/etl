import pandas as pd


df = pd.read_csv("fifa_s2.csv")
df1 = df.drop(columns="ID")
print(df1.info())

# удаляем неинформативные столбцы, где пустые значения занимают более 10%
# также все значения строковых столбцов приводим к нижнему регистру и удаляем
# дубли не заполненные ячейки с числовыми значениями заполняем средним
#  значением по столбцу
for i in df1.columns:
    if df1[i].count()/df["ID"].count() < 0.9:
        df1.drop(columns=i, inplace=True)
        continue
    if df1[i].dtype == object:
        df1[i] = df1[i].apply(lambda x: str(x).lower())
    elif df1[i].dtype != object:
        mean = df1[i].mean()
        df1[i] = df1[i].fillna(mean)
print(df1.info())
df1 = df1.drop_duplicates(subset=["Name", "Age"])


def add_age_groups(age) -> str:
    """Относит игрока к той или
    иной возрастной группе"""
    if age < 20:
        return "до 20"
    elif 20 <= age <= 30:
        return "от 20 до 30"
    elif 30 < age <= 36:
        return "от 30 до 36"
    elif 36 <= age:
        return "больше 36"
    else:
        return None

# используя ранее написанную функцию добавляем столбец с группировкой
# по возрасту игроков


df1["Age Groups"] = df1["Age"].apply(lambda x: add_age_groups(x))
print(df1.groupby("Age Groups")["Age Groups"].count())
# print(df1.info())
