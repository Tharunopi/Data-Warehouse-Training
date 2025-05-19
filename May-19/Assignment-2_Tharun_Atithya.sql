USE company

CREATE TABLE ProductInventory(
	ProductID INT IDENTITY(1, 1) PRIMARY KEY,
	ProductName VARCHAR(50),
	Category VARCHAR(20),
	Quantity INT,
	UnitPrice INT, 
	Supplier VARCHAR(50),
	LastRestocked DATE
)

INSERT INTO ProductInventory
VALUES 
	('Laptop', 'Electronics', 20, 70000, 'Tech Mart', '2025-04-20'),
	('Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
	('Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
	('Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
	('Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30')

SELECT * FROM ProductInventory

--1.1
INSERT INTO ProductInventory
VALUES 
	('Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01')

--1.2
UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp'

--1.3
DELETE FROM ProductInventory
WHERE ProductID = 2

--1.4
SELECT * FROM ProductInventory
ORDER BY ProductName ASC 

--2.5
SELECT * FROM ProductInventory
ORDER BY Quantity DESC 

--2.6
SELECT * FROM ProductInventory
WHERE Category = 'Electronics'

--2.7
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20

--2.8
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000

--3.9
SELECT ProductName, (Quantity * UnitPrice) AS TotalValue FROM ProductInventory

--3.10
SELECT Category, AVG(UnitPrice) AS AveragePrice FROM ProductInventory
GROUP BY Category

--3.11
SELECT Supplier, COUNT(ProductName) AS Counts FROM ProductInventory
GROUP BY Supplier 
HAVING Supplier = 'GadgetHub'

--4.12
SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%'

--4.13
SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000

--4.14 
SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000

--5.15 
SELECT TOP 3 * FROM ProductInventory
ORDER BY UnitPrice DESC

--5.16
SELECT * FROM ProductInventory
WHERE DATEDIFF(DAY, LastRestocked, GETDATE()) <= 10

--5.17
SELECT Supplier, SUM(Quantity) AS TotalQuantity FROM ProductInventory
GROUP BY Supplier

--5.18
SELECT * FROM ProductInventory
WHERE Quantity <= 30

--6.19
SELECT TOP 1 Supplier, COUNT(ProductName) AS NumOfProducts FROM ProductInventory
GROUP BY Supplier
ORDER BY COUNT(ProductName) DESC

--6.20
SELECT TOP 1 *, (Quantity * UnitPrice) AS TotalStockValue FROM ProductInventory
ORDER BY (Quantity * UnitPrice) DESC