SELECT * FROM Product

--1.1 
INSERT INTO Product
VALUES 
	('Gaming Keyboard', 'Electronics', 3500, 150, 'TechMart')

--1.2 
UPDATE Product
SET Price = Price + (Price * 0.10)
WHERE Category = 'Electronics'

--1.3 
DELETE FROM Product 
WHERE ProductID = 4

--1.4 
SELECT * FROM Product
ORDER BY Price DESC

--2.5
SELECT * FROM Product
ORDER BY StockQuantity ASC

--2.6
SELECT * FROM Product
WHERE Category = 'Electronics'

--2.7
SELECT * FROM Product
WHERE Category = 'Electronics' AND Price >= 5000

--2.8
SELECT * FROM Product
WHERE Category = 'Electronics' or Price <= 2000

--3.9
SELECT ProductName, (Price * StockQuantity) AS TotalStockValue FROM Product

--3.10
SELECT Category, AVG(Price) AS AveragePrice FROM Product
GROUP BY Category

--3.11
SELECT Supplier, COUNT(ProductName) AS TotalProducts FROM Product
GROUP BY Supplier

--4.12
SELECT * FROM Product
WHERE ProductName LIKE '%Wireless%'

--4.13
SELECT * FROM Product
WHERE Supplier = 'TechMart' OR Supplier = 'GadgetHub'

--4.14
SELECT * FROM Product
WHERE Price BETWEEN 1000 AND 20000

--5.15
SELECT ProductName, StockQuantity FROM Product
WHERE StockQuantity > (SELECT AVG(StockQuantity) FROM Product)

--5.16
SELECT TOP 3 * FROM Product
ORDER BY Price DESC

--5.17
SELECT Supplier, COUNT(ProductName) AS DuplicateSupply FROM Product
GROUP BY Supplier
HAVING COUNT(ProductName) > 1

--5.18
SELECT Category, COUNT(ProductName) AS NumberOfProducts, SUM(Price * StockQuantity) AS TotalStockValue FROM Product
GROUP BY Category

--6.19
SELECT Supplier, MaxProducts FROM (SELECT TOP 1 Supplier, COUNT(ProductName) AS MaxProducts FROM Product 
				  GROUP BY Supplier 
				  ORDER BY COUNT(ProductName) DESC) AS Stats

--6.20
SELECT Category, MAX(Price) AS HighestPrice FROM Product
GROUP BY Category
ORDER BY HighestPrice DESC