from typing import List, Dict, Any
from db import *

connection = get_db_connection()

def get_customers_by_credit_limit_range():
    cur = connection.cursor()
    query = """
        SELECT customerName, creditLimit
        FROM customers
        WHERE creditLimit < 10000 OR creditLimit > 100000;
    """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_orders_with_null_comments():
    cur = connection.cursor()
    query = """
                SELECT orderNumber, comments
                FROM orders
                WHERE comments IS NUll
                order by orderDate;
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_first_5_customers():
    cur = connection.cursor()
    query = """
                SELECT customerName, contactLastName, contactFirstName
                from customers
                order by contactLastName
                limit 5; 
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_payments_total_and_average():
    cur = connection.cursor()
    query = """
                select sum(amount), avg(amount), min(amount), max(amount)
                from payments;
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_employees_with_office_phone():
    cur = connection.cursor()
    query = """
                select employees.firstName, employees.lastName, offices.phone as officePhone
                from employees
                left join offices on offices.officeCode=employees.officeCode;
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_customers_with_shipping_dates():
    cur = connection.cursor()
    query = """
                select customers.customerName, orders.shippedDate
                from customers
                left join orders orders on customers.customerNumber=orders.customerNumber; 
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_customer_quantity_per_order():
    cur = connection.cursor()
    query = """
                select customers.customerName, orderdetails.quantityOrdered
                from customers
                join orders on customers.customerNumber=orders.customerNumber
                join orderdetails on orders.orderNumber=orderdetails.orderNumber
                order by customers.customerName;
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    return output


def get_customers_payments_by_lastname_pattern(pattern: str = "son"):
    cur = connection.cursor()
    query = """
                select customers.customerName, employees.firstName, employees.lastName, count(payments.amount) as sum_payments
                from customers
                join employees on employees.employeeNumber=customers.salesRepEmployeeNumber
                join payments on payments.customerNumber=customers.customerNumber
                where customers.contactFirstName = 'Mu%' or customers.contactFirstName = 'ly%'
                order by sum_payments;
            """
    cur.execute(query)
    output = cur.fetchall()
    connection.commit()
    cur.close()
    connection.close()
    return output