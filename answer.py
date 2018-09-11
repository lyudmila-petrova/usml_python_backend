import sqlite3

conn = sqlite3.connect('bottom.db')
cur = conn.cursor()

print('''
1. Посетители из какой страны совершают больше всего действий на сайте?
''')
cur.execute("""
    SELECT country_code, count(*) as cnt
    FROM hits h left join users u on h.ip = u.ip
    GROUP BY country_code
    ORDER BY count(country_code) DESC
    limit 5
""")
print(cur.fetchall())
print('------------------')

print('''
2. Посетители из какой страны чаще всего интересуются товарами из категории “fresh_fish”?
''')
cur.execute("""
    SELECT country_code, count(*) as cnt
    FROM hits h left join users u on h.ip = u.ip
    WHERE 
        h.product_category = 'fresh_fish'
        and h.action_type = 'category'
    GROUP BY country_code
    ORDER BY count(country_code) DESC
    limit 5
""")
print(cur.fetchall())
print('------------------')

print('''
3. В какое время суток чаще всего просматривают категорию “frozen_fish”?

(0) Ночь (00:00 - 06:00)
(1) Утро (06:00 - 12:00)
(2) День (12:00 - 18:00)
(3) Вечер (18:00 - 0:00)
''')
cur.execute('''
    select count(*) as cnt, substr(datetime, 12, 2) / 6 as val
    from hits h
    where 
        h.product_category = 'frozen_fish'
        and h.action_type = 'category'
    group by val
    order by cnt desc
''')
print(cur.fetchall())
print('------------------')

print('''
4. Какое максимальное число запросов на сайт за астрономический час (c 00 минут 00 секунд до 59 минут 59 секунд)?
''')
cur.execute('''
    select count(*) as cnt, substr(datetime, 0, 14) as dt
    from hits
    group by dt
    order by cnt desc
''')
print(cur.fetchone())
print('------------------')

print('''
5. Товары из какой категории чаще всего покупают совместно с товаром из категории “semi_manufactures”?
''')
cur.execute('''
    select product_category, count(*) as cnt 
    from order_items
    where order_id in(
            select distinct order_id 
            from order_items
            where product_category = 'semi_manufactures'
        )
        and product_category != 'semi_manufactures'
    group by product_category
    order by cnt desc
''')
print(cur.fetchall())
print('------------------')

print('''
6. Сколько брошенных (не оплаченных) корзин имеется? Укажите количество таких корзин.
''')
cur.execute('''
    select count(*) from orders where is_paid = 0
''')
print(cur.fetchone())
print('------------------')

print('''
7. Какое количество пользователей совершали повторные покупки?
''')
cur.execute('''
    select count(*) from (
        select ip
        from orders 
        where 
            is_paid = 1
        group by ip
        having count(*) > 1
    )
''')
print(cur.fetchone())
