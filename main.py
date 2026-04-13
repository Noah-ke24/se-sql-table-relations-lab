import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

queries = {
    "boston": """
        SELECT e.firstName, e.lastName
        FROM employees e
        JOIN offices o ON e.officeCode = o.officeCode
        WHERE o.city = 'Boston'
    """,

    "zero_emp": """
        SELECT o.officeCode, o.city
        FROM offices o
        LEFT JOIN employees e ON o.officeCode = e.officeCode
        WHERE e.employeeNumber IS NULL
    """,

    "employees": """
        SELECT e.firstName, e.lastName, o.city, o.state
        FROM employees e
        LEFT JOIN offices o ON e.officeCode = o.officecode
        ORDER BY e.firstName, e.lastName
    """,

    "no_orders": """
        SELECT c.contactFirstName, c.contactLastName, c.phone
        FROM customers c
        LEFT JOIN orders o ON c.customerNumber = o.customerNumber
        WHERE o.orderNumber IS NULL
    """,

    "payments": """
        SELECT c.contactFirstName, c.contactLastName, p.amount
        FROM customers c
        JOIN payments p ON c.customerNumber = p.customerNumber
        ORDER BY CAST(p.amount AS REAL) DESC
    """,

    "credit": """
        SELECT e.employeeNumber, e.firstName, e.lastName
        FROM employees e
        JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
        GROUP BY e.employeeNumber
        HAVING AVG(c.creditLimit) > 90000
    """,

    "products_sold": """
        SELECT p.productName, SUM(od.quantityOrdered) AS totalunits
        FROM products p
        JOIN orderdetails od ON p.productCode = od.productCode
        GROUP BY p.productCode
        ORDER BY totalunits DESC
    """,

    "product_customers": """
        SELECT p.productName, COUNT(DISTINCT o.customerNumber) AS buyers
        FROM products p
        JOIN orderdetails od ON p.productCode = od.productCode
        JOIN orders o ON od.orderNumber = o.orderNumber
        GROUP BY p.productCode
    """,

    "customers_per_office": """
        SELECT o.city, COUNT(c.customerNumber) AS total
        FROM offices o
        JOIN employees e ON o.officeCode = e.officeCode
        JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
        GROUP BY o.officeCode
    """,

    "under_20": """
        SELECT DISTINCT e.firstName, e.lastName
        FROM employees e
        JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
        JOIN orders o ON c.customerNumber = o.customerNumber
        JOIN orderdetails od ON o.orderNumber = od.orderNumber
        WHERE od.productCode IN (
            SELECT productCode
            FROM orderdetails
            GROUP BY productCode
            HAVING COUNT(DISTINCT orderNumber) < 20
        )
    """
}

# Run all queries
results = {name: pd.read_sql(q, conn) for name, q in queries.items()}

conn.close()