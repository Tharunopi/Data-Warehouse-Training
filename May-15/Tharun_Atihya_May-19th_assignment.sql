--1.1
INSERT INTO Book
VALUES 
	('Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35)

--1.2
UPDATE Book 
SET Price = Price + 50
WHERE Genre = 'Self-Help'

--1.3
DELETE FROM Book
WHERE BookID = 4

--1.4
SELECT * FROM Book
ORDER BY Title ASC

--2.5
SELECT * FROM Book
ORDER BY Price DESC

--2.6
SELECT * FROM Book
WHERE Genre = 'Fiction'

--2.7 
SELECT * FROM Book
WHERE Genre = 'Self-Help' AND Price >= 400

--2.8
SELECT * FROM Book
WHERE Genre = 'Fiction' OR PublishedYear >= 2000

--3.9
SELECT Title, (Price * Stock) AS TotalValue FROM Book
WHERE Stock > 0

--3.10
SELECT Genre, AVG(Price) AS AveragePrice FROM Book
GROUP BY Genre

--3.11
SELECT Author, COUNT(Title) AS NumberOfBooks FROM Book
GROUP BY Author 
HAVING Author = 'Paulo Coelho'

--4.12
SELECT * FROM Book
WHERE Title LIKE '%The%'

--4.13
SELECT * FROM Book
WHERE Author = 'Yuval Noah Harari' AND Price <= 600

--4.14
SELECT * FROM Book
WHERE Price BETWEEN 300 AND 500

--5.15
SELECT TOP 3 * FROM Book
ORDER BY Price DESC 

--5.16
SELECT * FROM Book
WHERE PublishedYear <= 2000

--5.17
SELECT Genre, COUNT(Title) AS NumberOfBooks FROM Book
GROUP BY Genre

--5.18
SELECT Title, COUNT(Title) AS Duplicate FROM Book
GROUP BY Title
HAVING COUNT(Title) > 1

--6.19 (Using Sub Query)
SELECT Author, NumberOfBooks FROM (SELECT TOP 1 Author, COUNT(Title) AS NumberOfBooks FROM Book
				GROUP BY Author  
				ORDER BY COUNT(Title) DESC) AS Stats

--6.20
SELECT Genre, MIN(PublishedYear) AS EarliestPublishedBook FROM Book
GROUP BY Genre 
