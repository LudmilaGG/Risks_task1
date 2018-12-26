# coding: utf-8
import mysql.connector
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

conn = mysql.connector.connect(host='localhost',database='my_db',user='my_db_user')

cur = conn.cursor()

payments_df = pd.read_excel("payments.xls")

APPLICATIONS_TABLE = 'applications'
CONTRACTS_TABLE = 'contracts'
PAYMENTS_TABLE = 'payments'
DEFAULTS_TABLE = 'defaults'

cur.execute("""
DROP TABLE IF EXISTS %s
""" % PAYMENTS_TABLE)

cur.execute("""
DROP TABLE IF EXISTS %s
""" % DEFAULTS_TABLE)

cur.execute("""
CREATE TABLE %s (
    id INT AUTO_INCREMENT,
    contract_number INT NOT NULL,
    date DATE,
    amnt_due FLOAT,
    amnt_paid FLOAT,
    FOREIGN KEY (contract_number) REFERENCES %s(contract_number) ON DELETE CASCADE,
    PRIMARY KEY (id)
)  ENGINE=INNODB;
""" % (PAYMENTS_TABLE, CONTRACTS_TABLE))

cur.execute("""
CREATE TABLE %s (
    id_number INT NOT NULL,
    default_date DATE,
    FOREIGN KEY (id_number) REFERENCES %s(id_number) ON DELETE CASCADE,
    PRIMARY KEY (id_number)
)  ENGINE=INNODB;
""" % (DEFAULTS_TABLE, APPLICATIONS_TABLE))

no_contracts = set()
for ind in payments_df.index:
    row = payments_df.loc[ind]
    cur.execute("SELECT 1=1 FROM %s WHERE contract_number=%d" % (CONTRACTS_TABLE, int(row['Contract Number'])))
    if cur.fetchone():
        cur.execute("INSERT INTO %s (contract_number, date, amnt_due, amnt_paid) VALUES (%d, '%s', %f, %f)" % (PAYMENTS_TABLE,
                                                                                                         row['Contract Number'],
                                                                                                         row['Date'],
                                                                                                         row['Amount Due'],
                                                                                                         row['Amount Paid']))
    else:
        no_contracts.add(int(row['Contract Number']))
print("WARNING! There are no contracts with numbers:", no_contracts)
conn.commit()

payments_df = pd.read_sql_query("SELECT * FROM %s" % PAYMENTS_TABLE, conn, index_col='id')

s_dates = payments_df.groupby(['contract_number'], as_index=False).agg({'date': 'min'})

s_dates['amnt_due'] = np.nan
s_dates['amnt_paid'] = np.nan

for ind in s_dates.index:
    row = s_dates.loc[ind]
    t_df = payments_df[(payments_df.contract_number == row.contract_number) & (payments_df.date == row.date)]
    s_dates.loc[ind, 'amnt_due'] = t_df.amnt_due.iloc[0]
    s_dates.loc[ind, 'amnt_paid'] = t_df.amnt_paid.iloc[0]

if np.any(s_dates.amnt_paid - s_dates.amnt_due < 0):
    mb_zero_default = s_dates[s_dates.amnt_paid - s_dates.amnt_due < 0].contract_number.unique()
    print("There may be overdues in the first period! Check contracts:", mb_zero_default)

# Заполняем количество дней просрочки

payments_df['overdue_days'] = -1  # Ставим -1, там, где не было просрочки (потом уберем), а 0 будем ставить там, где должен был быть платеж, но не произошёл

for contract in payments_df.contract_number.unique():
    temp_df = payments_df[payments_df.contract_number == contract].sort_values(['date'])
    # Получаем массив индексов, т.к. индексация в "частях" pandas.DataFrame остаётся как и в исходном
    index_arr = temp_df.index.tolist()
    for i in range(1, len(index_arr)):  # Берем индексы с 1, т.к. в 0 всегда просрочка 0 (первый платеж)
        prev_ind = index_arr[i - 1]
        ind = index_arr[i]
        if payments_df.loc[ind, 'amnt_paid'] < payments_df.loc[ind, 'amnt_due']:  # Платеж не поступил или был меньше
            if payments_df.loc[prev_ind, 'overdue_days'] == -1:  # Не было просрочки
                payments_df.loc[ind, 'overdue_days'] = 0  # С текущей даты пошла просрочка, но пока что 0
            else:
#                 new_ov_days = (payments_df.loc[ind, 'date'] - payments_df.loc[prev_ind, 'date']).days  # Вычисляем кол-во дней просрочки
                #  Для простоты будем просто добавлять 30, т.к. платежи ровно через 1 мес.
                new_ov_days = 30
                payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] + new_ov_days  # Складываем прошлую просрочку с текущей
        elif payments_df.loc[ind, 'amnt_paid'] > payments_df.loc[ind, 'amnt_due']:
            months_covered = int(round(payments_df.loc[ind, 'amnt_paid'] / payments_df.loc[ind, 'amnt_due'], 0))  # Считаем, за сколько месяцев оплатил
            months_covered = months_covered - 1  # Вычитаем 1 месяца из погашения, т.к. мы не прибавили 30 дней за текущий месяц
            # Вычитаем из просрочки количество "погашенных" просроченных платежей
            # В случае более сложного начисления (с процентами, неравномерные платежи) надо использовать логику сложнее
            payments_df.loc[ind, 'overdue_days'] = payments_df.loc[prev_ind, 'overdue_days'] - months_covered * 30
            if payments_df.loc[ind, 'overdue_days'] < 0:
                payments_df.loc[ind, 'overdue_days'] = -1  # Если погасил весь долг - нет просрочки

# Берем макс. дату выхода в просрочку 90+, т.к. вдруг клиент гасил просрочку 90 и возрашался в 60
default_dates = payments_df[payments_df.overdue_days == 90].groupby(['contract_number'], as_index=False).agg({"date": "max"})

print(default_dates.rename({"contract_number": "Contract Number", "date": "Default Date"}, axis=1))


# ### 3.b
contracts_df = pd.read_sql_query("SELECT * FROM %s" % CONTRACTS_TABLE, conn, index_col='contract_number')

# Определяем дату договора и заёмщика
payments_df['contract_date'] = np.nan
payments_df['id_number'] = 0
for ind in payments_df.index:
    row = payments_df.loc[ind]
    payments_df.loc[ind, 'contract_date'] = contracts_df.loc[row.contract_number].contract_date
    payments_df.loc[ind, 'id_number'] = contracts_df.loc[row.contract_number].id_number

# Определяем "возраст" договора в месяцах на каждую дату
payments_df['age'] = 0
for contract in payments_df.contract_number.unique():
    temp_df = payments_df[payments_df.contract_number == contract].sort_values(['date'])
    index_arr = temp_df.index.tolist()
    contract_date = temp_df.contract_date.iloc[0]  # Берем первую дату договора из таблицы, т.к. они все одинаковые
    for i in range(0, len(index_arr)):
        ind = index_arr[i]
        curr_date = payments_df.loc[ind, 'date']
        curr_age = (curr_date.year - contract_date.year) * 12 + (curr_date.month - contract_date.month)
        payments_df.loc[ind, 'age'] = curr_age
# Такой алгоритм вычисления возраста не подойдёт для данных, у которых частота чаще, чем 1 раз в месяц

period_months = int(input("Please enter risk horizon (in months): "))

out_df = payments_df[payments_df.age == period_months][['contract_number',
                                                        'date',
                                                        'contract_date',
                                                        'id_number',
                                                        'age']].reset_index(drop=True)  # Берем только нужные поля и "сбрасываем" индекс (начинаем с 0)

out_df['Default?'] = False

for contract in out_df.contract_number.unique():
    if contract in default_dates.contract_number.values:
        default_date = default_dates[default_dates.contract_number == contract].date.iloc[0]
        current_date = out_df.loc[out_df.contract_number == contract, "date"].iloc[0]
        if current_date >= default_date:
            out_df.loc[(out_df.contract_number == contract), "Default?"] = True

# print(out_df.sort_values(['id_number', 'contract_number']).reset_index(drop=True).rename({"contract_number": "Contract",
#                                                                                     "date": "Date",
#                                                                                     "contract_date": "Contract Date",
#                                                                                     "id_number": "Person ID",
#                                                                                     "age": "Age (months)"}, axis=1)[['Person ID', 'Age (months)', 'Default?']])

print(out_df.groupby(['id_number']).agg({"age": "max", "Default?": "max"}).rename({"age": "Age (months)"}, axis=1))

# Определяем клиентов-дефолтников
contracts_filter = ', '.join(default_dates.contract_number.unique().astype(str))
default_data = pd.read_sql_query("""
SELECT
     id_number,
     contract_number
FROM
    %s
WHERE
    contract_number IN (%s)
""" % (CONTRACTS_TABLE, contracts_filter), conn)

default_data

default_data['default_date'] = default_data.contract_number.apply(lambda x: default_dates.set_index('contract_number').loc[x].date)

default_clients = set(default_data.id_number.values)

# Выбираем нужные данные о клиентах
applications_df = pd.read_sql_query("""
SELECT
    *
FROM
    %s
""" % APPLICATIONS_TABLE, conn, index_col='id_number')

# Убираем ненужные колонки
applications_df.drop(["name", "application_date", 'city'], axis=1, inplace=True)

# Убираем клиента, по которому мало данных

applications_df.drop([100076], inplace=True)

# Заполняем пустые поля
applications_df.fillna(0.0, inplace=True)

applications_df['default'] = 0

applications_df.loc[applications_df.index.isin(default_clients), "default"] = 1

applications_df['age'] = (datetime.date.today() - applications_df.birth_date).apply(lambda x: x.days // 365)

applications_df.drop(['birth_date'], axis=1, inplace=True)

applications_df.income = pd.qcut(applications_df.income, 5)

def get_age_of_car_category(age):
    if age == 0:
        return '0'
    elif age <= 3:
        return '<=3'
    else:
        return '>3'

applications_df.age_of_car = applications_df.age_of_car.apply(get_age_of_car_category)

applications_df.age = pd.qcut(applications_df.age, 5)

quantilized_columns = ['income', 'age', 'age_of_car']

categorucal_columns = ['gender',
                       'employed_by',
                       'education',
                       'marital_status',
                       'position',
                       'income_type',
                       'housing',
                       'house_ownership',
                       'children',
                       'family',
                       ]

choices = dict(zip(range(len(categorucal_columns + quantilized_columns)), sorted(categorucal_columns + quantilized_columns)))

for i in choices.keys():
    print("%d: %s" % (i, choices[i]))

column_choice = choices.get(int(input("Please choose column (default=0): ")), 0)

coded_columns = categorucal_columns.copy()

coded_columns.remove('gender')
coded_columns.remove('house_ownership')
coded_columns.remove('children')
coded_columns.remove('family')

reverse_dicts = dict()
for column_name in coded_columns:
    cur.execute("SELECT * FROM %s_dict" % (column_name))
    temp_dict = {i[0]: i[1] for i in cur.fetchall()}
    reverse_dicts[column_name] = temp_dict 

for column_name in coded_columns:
    applications_df[column_name] = applications_df[column_name + '_id'].apply(lambda x: reverse_dicts[column_name][x])
    applications_df.drop([column_name + '_id'], axis=1, inplace=True)

temp_df = applications_df[[column_choice, 'default']].copy()

temp_df['event'] = temp_df.default
temp_df['non_event'] = 1 - temp_df.default

df_gb = temp_df.groupby([column_choice]).agg({"default": "sum",
                                         "event": lambda x: x.sum() / temp_df.event.sum(),
                                         "non_event": lambda x: x.sum() / temp_df.non_event.sum()})

df_gb['woe'] = np.log(np.clip(df_gb.event / df_gb.non_event, 0.001, np.inf))

df_gb['IV'] = (df_gb.event - df_gb.non_event) * df_gb.woe

df_gb.index = df_gb.index.astype(str)

plt.figure(figsize=(12, 7));
plt.plot(list(range(df_gb.shape[0])), df_gb.woe, 'o-')
plt.grid()
plt.xticks(list(range(df_gb.shape[0])), df_gb.index)
plt.ylabel("WOE", fontsize=14)
plt.xlabel(column_choice, fontsize=14)
plt.title("WOE plot", fontsize=16)
plt.ion()
plt.show()

plt.figure(figsize=(12, 7));
plt.plot(list(range(df_gb.shape[0])), df_gb.IV, 'o-')
plt.grid()
plt.xticks(list(range(df_gb.shape[0])), df_gb.index)
plt.ylabel("IV", fontsize=14)
plt.xlabel(column_choice, fontsize=14)
plt.title("Information Value plot", fontsize=16)
plt.ion()
plt.show()

df_gb.loc['SUM'] = df_gb.sum()

print(df_gb)

def_gb = default_data.groupby(['id_number'], as_index = False).agg({"default_date": "min"})

for id_number, default_date in def_gb.values:
    cur.execute("""
    INSERT INTO %s VALUES (%d, '%s')
    """ % (DEFAULTS_TABLE, id_number, default_date))

conn.commit()

plt.pause(0.001)
input("Press [enter] to continue...")
