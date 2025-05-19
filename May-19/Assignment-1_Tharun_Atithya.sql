USE company

CREATE TABLE EmployeeAttendance(
	AttendanceID INT IDENTITY(1,1) PRIMARY KEY,
	EmployeeName VARCHAR(50),
	Department VARCHAR(20),
	Date DATE,
	Status VARCHAR(20),
	HoursWorked INT
)

INSERT INTO EmployeeAttendance
VALUES 
	('John Doe', 'IT', '2025-05-01', 'Present', 8),
	('Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
	('Ali Khan', 'IT', '2025-05-01', 'Present', 7),
	('Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
	('Daivd Brown', 'Marketing', '2025-05-01', 'Present', 8)

SELECT * FROM EmployeeAttendance

--1.1
INSERT INTO EmployeeAttendance
VALUES
	('Neha Sharma', 'Finance', '2025-05-01', 'Present', 8)

--1.2
UPDATE EmployeeAttendance
SET Status = 'Present'
WHERE EmployeeName = 'Riya Patel'

--1.3
DELETE FROM EmployeeAttendance
WHERE EmployeeName = 'Priya Singh' AND Date='2025-05-01'

--1.4
SELECT * FROM EmployeeAttendance
ORDER BY EmployeeName ASC

--2.5
SELECT * FROM EmployeeAttendance
ORDER BY HoursWorked DESC

--2.6
SELECT * FROM EmployeeAttendance
WHERE Department = 'IT'

--2.7
SELECT * FROM EmployeeAttendance
WHERE Department = 'IT' AND Status = 'Present'

--2.8
SELECT * FROM EmployeeAttendance
WHERE Status = 'Late' OR Status = 'Absent'

--3.9
SELECT Department, SUM(HoursWorked) AS TotalHoursWorked FROM EmployeeAttendance
GROUP BY Department

--3.10
--selecting a particular day('01-05-2025') to calculate average hours
SELECT AVG(HoursWorked) AS AvergageWorkHours FROM EmployeeAttendance
WHERE DATE = '2025-05-01'

--3.11
SELECT Status, COUNT(Status) AS Counts FROM EmployeeAttendance
GROUP BY Status

--4.12
SELECT * FROM EmployeeAttendance
WHERE EmployeeName LIKE 'R%'

--4.13
SELECT * FROM EmployeeAttendance
WHERE HoursWorked > 6 AND Status = 'Present'

--4.14
SELECT * FROM EmployeeAttendance
WHERE HoursWorked BETWEEN 6 AND 8

--5.15
SELECT TOP 2 EmployeeName, SUM(HoursWorked) AS HoursWorked FROM EmployeeAttendance
GROUP BY EmployeeName
ORDER BY SUM(HoursWorked) DESC

--5.16
SELECT * FROM EmployeeAttendance
WHERE HoursWorked < (SELECT AVG(HoursWorked) FROM EmployeeAttendance)

--5.17
SELECT Status, AVG(HoursWorked) AS AverageHours FROM EmployeeAttendance
GROUP BY Status

--5.18
--selecting a particular day('01-05-2025') 
SELECT EmployeeName, COUNT(Status) AS Counts FROM EmployeeAttendance
GROUP BY EmployeeName
HAVING COUNT(Status) > 1

--6.19
SELECT TOP 1 Department, COUNT(EmployeeName) AS EmployeeCount FROM EmployeeAttendance
GROUP BY Department
ORDER BY COUNT(EmployeeName) DESC

--6.20
SELECT e1.* FROM EmployeeAttendance e1
JOIN (SELECT Department, MAX(HoursWorked) AS MaxHours FROM EmployeeAttendance
      GROUP BY Department) e2
ON e1.Department = e2.Department AND e1.HoursWorked = e2.MaxHours;