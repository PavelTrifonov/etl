import pandas as pd

# Приведем таблицу data ко 2 нормальной форме

data = {
    'Employee_ID': ['E001', 'E001', 'E002', 'E002', 'E003'],
    'Name': ['Alice', 'Alice', 'Bob', 'Bob', 'Alice'],
    'Job_Code': ['J01', 'J02', 'J02', 'J03', 'J01'],
    'Job': ['Chef', 'Waiter', 'Waiter', 'Bartender', 'Chef'],
    'City_code': [26, 26, 56, 56, 56],
    'Home_city': ['Moscov', 'Moscov', 'Perm', 'Perm', 'Perm']
}

# Создаем первую таблицу с столбцами "Employee_ID", "Name", "Job_Code" и "Job"
table1 = pd.DataFrame({
    'Employee_ID': data['Employee_ID'],
    'Name': data['Name'],
    'Job_Code': data['Job_Code'],
    'Job': data['Job']
}).drop_duplicates()

# Создаем вторую таблицу с столбцами "City_code" и "Home_city"
table2 = pd.DataFrame({
    'City_code': data['City_code'],
    'Home_city': data['Home_city']
}).drop_duplicates()

'''Теперь данные разделены на две таблицы, где каждая таблица содержит только
независимые столбцы. Таблица 1 содержит информацию о сотрудниках, их
идентификаторах, их именах, кодах работы и названиях работы. Таблица 2
содержит информацию о городе и домашнем городе сотрудника на основе
кода города.'''

# Выводим результат
print("Таблица 1:")
print(table1)
print()
print("Таблица 2:")
print(table2)

# Приведем таблицу data к 3 нормальной форме

data = {
    'Employee_ID': ['E001', 'E001', 'E002', 'E002', 'E003'],
    'Name': ['Alice', 'Alice', 'Bob', 'Bob', 'Alice'],
    'Job_Code': ['J01', 'J02', 'J02', 'J03', 'J01'],
    'Job': ['Chef', 'Waiter', 'Waiter', 'Bartender', 'Chef'],
    'City_code': [26, 26, 56, 56, 56],
    'Home_city': ['Moscov', 'Moscov', 'Perm', 'Perm', 'Perm']
}

# Создаем первую таблицу с столбцами "Employee_ID", "Name" и "Job_Code"
table1 = pd.DataFrame({
    'Employee_ID': data['Employee_ID'],
    'Name': data['Name'],
    'Job_Code': data['Job_Code']
}).drop_duplicates()

# Создаем вторую таблицу с столбцами "Job_Code" и "Job"
table2 = pd.DataFrame({
    'Job_Code': data['Job_Code'],
    'Job': data['Job']
}).drop_duplicates()

# Создаем третью таблицу с столбцами "City_code" и "Home_city"
table3 = pd.DataFrame({
    'City_code': data['City_code'],
    'Home_city': data['Home_city']
}).drop_duplicates()

'''Теперь данные разделены на три таблицы, где каждая таблица содержит
только независимые столбцы. Таблица 1 содержит информацию о сотрудниках,
их идентификаторах и рабочих кодах. Таблица 2 содержит информацию о
рабочих кодах и соответствующих работах. Таблица 3 содержит информацию о
кодах городов и соответствующих домашних городах.'''

# Выводим результат
print("Таблица 1:")
print(table1)
print()
print("Таблица 2:")
print(table2)
print()
print("Таблица 3:")
print(table3)
