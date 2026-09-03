use Northwind
go 

select 
row_number() over (order by (select null)) as GeographyKey
,* from(
select c.Country,c.Region, c.City, c.PostalCode, c.[Address]  from dbo.Customers as c 
union 
select Country,Region, City, PostalCode, [Address]  from dbo.Employees 
union 
select Country,Region, City, PostalCode, [Address]  from dbo.Suppliers
union 
select shipCountry,shipRegion, shipCity, ShipPostalCode, ShipAddress  from dbo.Orders ) as x 

SELECT        Orders.OrderID, [Order Details].ProductID, Orders.CustomerID, Orders.EmployeeID, Orders.OrderDate, Orders.RequiredDate, Orders.ShippedDate, Orders.ShipVia, Orders.ShipName
FROM            Orders INNER JOIN
                         [Order Details] ON Orders.OrderID = [Order Details].OrderID

select * from dbo.Categories as c 
where c.CategoryID not in (select p.CategoryID  from dbo.Products as p) 