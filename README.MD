## Решение

Файл `main.py` содержит функцию-решение `add_session_id_column`. Такой вариант получился не самым понятным, но самым быстрым из
тех решений, которые удалось получить только с `pandas` и `numpy` (на домашнем ПК функция работает около 2 мин. на 100 млн. строк). Решение предполагает, что все записи влезут в оперативную память.

## Задание No1.

Есть Pandas DataFrame со столбцами [“customer_id”, “product_id”, “timestamp”] , который содержит данные по просмотрам товаров на сайте. Есть проблема – просмотры одного customer_id не разбиты на сессии (появления на сайте). Мы хотим разместить сессии так, чтобы сессией считались все смежные просмотры, между которыми не более 3 минут.

Написать методом который создаст в Pandas DataFrame столбец session_id и проставит в нем уникальный int id для каждой сессии.

У каждого пользователя может быть по несколько сессий. Исходный DataFrame может быть большим – до 100 млн строк.