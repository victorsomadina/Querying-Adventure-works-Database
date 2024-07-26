
# task 1.1

WITH
  latest_address AS (
  SELECT
    customer_address.CustomerID,
    MAX(customer_address.AddressID) AS LatestAddressID
  FROM
    `adwentureworks_db.customeraddress` customer_address
  GROUP BY
    customer_address.CustomerID )
SELECT
  DISTINCT individual.CustomerID,
  contact.Firstname,
  contact.LastName,
  (contact.Firstname ||" "|| contact.Lastname) Full_Name,
  CASE
    WHEN contact.Title IS NULL THEN "Dear"
    ELSE contact.Title
END
  AS Addressing_Title,
  contact.EmailAddress,
  contact.Phone,
  customer.accountnumber,
  customer.customertype,
  address.addressline1 AS Address,
  address.city,
  state_province.name AS State,
  country.name AS Country,
  COUNT(salesorderid) OVER(PARTITION BY individual.CustomerID) AS Number_of_Orders,
  ROUND(SUM(TotalDue) OVER(PARTITION BY individual.CustomerID), 2) AS Total_Amount,
  MAX(Orderdate) OVER(PARTITION BY individual.CustomerID) AS Date_of_Last_order
FROM
  `adwentureworks_db.individual` individual
JOIN
  `adwentureworks_db.contact` contact
ON
  individual.ContactID = contact.ContactId
LEFT JOIN
  `adwentureworks_db.customer` customer
ON
  individual.CustomerID = customer.CustomerID
JOIN
  latest_address
ON
  latest_address.customerid = customer.CustomerID
JOIN
  `adwentureworks_db.address` address
ON
  address.AddressID = latest_address.latestAddressID
JOIN
  `adwentureworks_db.stateprovince`state_province
ON
  state_province.stateprovinceid = address.StateProvinceID
JOIN
  `adwentureworks_db.countryregion` country
ON
  country.countryregioncode = state_province.countryregioncode
JOIN
  `adwentureworks_db.salesorderheader` sales_order
ON
  individual.CustomerID = sales_order.CustomerID
ORDER BY
  Total_Amount DESC
LIMIT
  200;


# task 1.2
WITH
  new_table AS (
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS LatestAddressID
    FROM
      `adwentureworks_db.customeraddress` customer_address
    GROUP BY
      customer_address.CustomerID )
  SELECT
    DISTINCT individual.CustomerID,
    contact.Firstname,
    contact.LastName,
    (contact.Firstname ||" "|| contact.Lastname) Full_Name,
    CASE
      WHEN contact.Title IS NULL THEN "Dear"
      ELSE contact.Title
  END
    AS Addressing_Title,
    contact.EmailAddress,
    contact.Phone,
    customer.accountnumber,
    customer.customertype,
    address.addressline1 AS Address,
    address.city,
    state_province.name AS State,
    country.name AS Country,
    COUNT(salesorderid) OVER(PARTITION BY individual.CustomerID) AS Number_of_Orders,
    ROUND(SUM(TotalDue) OVER(PARTITION BY individual.CustomerID), 2) AS Total_Amount,
    MAX(Orderdate) OVER(PARTITION BY individual.CustomerID) AS Date_of_Last_order
  FROM
    `adwentureworks_db.individual` individual
  JOIN
    `adwentureworks_db.contact` contact
  ON
    individual.ContactID = contact.ContactId
  LEFT JOIN
    `adwentureworks_db.customer` customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    latest_address
  ON
    latest_address.customerid = customer.CustomerID
  JOIN
    `adwentureworks_db.address` address
  ON
    address.AddressID = latest_address.latestAddressID
  JOIN
    `adwentureworks_db.stateprovince`state_province
  ON
    state_province.stateprovinceid = address.StateProvinceID
  JOIN
    `adwentureworks_db.countryregion` country
  ON
    country.countryregioncode = state_province.countryregioncode
  JOIN
    `adwentureworks_db.salesorderheader` sales_order
  ON
    individual.CustomerID = sales_order.CustomerID)
SELECT
  *
FROM
  new_table
WHERE
  Date_of_Last_order < DATE_SUB((
    SELECT
      MAX(orderdate)
    FROM
      `adwentureworks_db.salesorderheader`), INTERVAL 365 DAY)
ORDER BY
  Total_Amount DESC
LIMIT
  200;


# task 1.3
WITH
  new_table AS (
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS LatestAddressID
    FROM
      `adwentureworks_db.customeraddress` customer_address
    GROUP BY
      customer_address.CustomerID )
  SELECT
    DISTINCT individual.CustomerID,
    contact.Firstname,
    contact.LastName,
    (contact.Firstname ||" "|| contact.Lastname) Full_Name,
    CASE
      WHEN contact.Title IS NULL THEN "Dear"
      ELSE contact.Title
  END
    AS Addressing_Title,
    contact.EmailAddress,
    contact.Phone,
    customer.accountnumber,
    customer.customertype,
    address.addressline1 AS Address,
    address.city,
    state_province.name AS State,
    country.name AS Country,
    COUNT(salesorderid) OVER(PARTITION BY individual.CustomerID) AS Number_of_Orders,
    ROUND(SUM(TotalDue) OVER(PARTITION BY individual.CustomerID), 2) AS Total_Amount,
    MAX(Orderdate) OVER(PARTITION BY individual.CustomerID) AS Date_of_Last_order
  FROM
    `adwentureworks_db.individual` individual
  JOIN
    `adwentureworks_db.contact` contact
  ON
    individual.ContactID = contact.ContactId
  LEFT JOIN
    `adwentureworks_db.customer` customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    latest_address
  ON
    latest_address.customerid = customer.CustomerID
  JOIN
    `adwentureworks_db.address` address
  ON
    address.AddressID = latest_address.latestAddressID
  JOIN
    `adwentureworks_db.stateprovince`state_province
  ON
    state_province.stateprovinceid = address.StateProvinceID
  JOIN
    `adwentureworks_db.countryregion` country
  ON
    country.countryregioncode = state_province.countryregioncode
  JOIN
    `adwentureworks_db.salesorderheader` sales_order
  ON
    individual.CustomerID = sales_order.CustomerID)
SELECT
  *,
  CASE
    WHEN Date_of_Last_order < DATE_SUB(( SELECT MAX(orderdate) FROM `adwentureworks_db.salesorderheader`), INTERVAL 365 DAY) THEN "Inactive"
    ELSE "Active"
END
  AS Activity_Status
FROM
  new_table
ORDER BY
  customerid DESC
LIMIT
  500;

# task 1.4 

WITH
  new_table AS (
  WITH
    latest_address AS (
    SELECT
      customer_address.CustomerID,
      MAX(customer_address.AddressID) AS LatestAddressID
    FROM
      `adwentureworks_db.customeraddress` customer_address
    GROUP BY
      customer_address.CustomerID )
  SELECT
    DISTINCT individual.CustomerID,
    contact.Firstname,
    contact.LastName,
    (contact.Firstname ||" "|| contact.Lastname) Full_Name,
    CASE
      WHEN contact.Title IS NULL THEN "Dear"
      ELSE contact.Title
  END
    AS Addressing_Title,
    contact.EmailAddress,
    contact.Phone,
    customer.accountnumber,
    customer.customertype,
    customer.TerritoryID,
    address.addressline1 AS Address,
    address.city,
    state_province.name AS State,
    country.name AS Country,
    COUNT(salesorderid) OVER(PARTITION BY individual.CustomerID) AS Number_of_Orders,
    ROUND(SUM(TotalDue) OVER(PARTITION BY individual.CustomerID), 2) AS Total_Amount,
    MAX(Orderdate) OVER(PARTITION BY individual.CustomerID) AS Date_of_Last_order
  FROM
    `adwentureworks_db.individual` individual
  JOIN
    `adwentureworks_db.contact` contact
  ON
    individual.ContactID = contact.ContactId
  LEFT JOIN
    `adwentureworks_db.customer` customer
  ON
    individual.CustomerID = customer.CustomerID
  JOIN
    latest_address
  ON
    latest_address.customerid = customer.CustomerID
  JOIN
    `adwentureworks_db.address` address
  ON
    address.AddressID = latest_address.latestAddressID
  JOIN
    `adwentureworks_db.stateprovince`state_province
  ON
    state_province.stateprovinceid = address.StateProvinceID
  JOIN
    `adwentureworks_db.countryregion` country
  ON
    country.countryregioncode = state_province.countryregioncode
  JOIN
    `adwentureworks_db.salesorderheader` sales_order
  ON
    individual.CustomerID = sales_order.CustomerID),
  activity_table AS (
  SELECT
    *,
    CASE
      WHEN Date_of_Last_order < DATE_SUB(( SELECT MAX(orderdate) FROM `adwentureworks_db.salesorderheader`), INTERVAL 365 DAY) THEN "Inactive"
      ELSE "Active"
  END
    AS Activity_Status
  FROM
    new_table),
  Group_table AS (
  SELECT
    customerid,
    firstname,
    lastname,
    full_name,
    addressing_title,
    emailaddress,
    phone,
    accountnumber,
    customertype,
    address,
    SUBSTR(address, 1, STRPOS(address, " ")) AS Address_No,
    SUBSTR(address, STRPOS(address, " "), LENGTH(address)) AS Address_ST,
    city,
    state,
    country,
    number_of_orders,
    total_amount,
    date_of_last_order,
    activity_status,
    sales_territory.Name AS Territory_Name,
    sales_territory.Group
  FROM
    activity_table
  JOIN
    `adwentureworks_db.salesterritory` sales_territory
  ON
    activity_table.Territoryid = sales_territory.TerritoryID
  WHERE
    activity_status = 'Active'
    AND sales_territory.Group = 'North America')
SELECT
  *
FROM
  Group_table
WHERE
  Total_Amount >= 2500
  OR number_of_orders >= 5
ORDER BY
  country,
  state,
  date_of_last_order;

# task 2.1

WITH
  Sales AS (
  SELECT
    LAST_DAY(DATE(OrderDate)) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorder.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorder.CustomerID) AS number_customers,
    COUNT(DISTINCT salesperson.SalesPersonID) AS no_salesPersons,
    ROUND(SUM(salesorder.TotalDue), 2) AS Total_w_tax
  FROM
    adwentureworks_db.salesorderheader AS salesorder
  LEFT JOIN
    adwentureworks_db.salesterritory AS salesterritory
  ON
    salesterritory.TerritoryID = salesorder.TerritoryID
  LEFT JOIN
    adwentureworks_db.salesperson AS salesperson
  ON
    salesperson.SalesPersonID = salesorder.SalesPersonID
  GROUP BY
    LAST_DAY(DATE(salesorder.OrderDate)),
    salesterritory.CountryRegionCode,
    salesterritory.name )

SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax
FROM
  Sales
ORDER BY
  order_month ASC;

# task 2.2



WITH
  Sales AS (
  SELECT
    LAST_DAY(DATE(OrderDate)) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorder.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorder.CustomerID) AS number_customers,
    COUNT(DISTINCT salesperson.SalesPersonID) AS no_salesPersons,
    ROUND(SUM(salesorder.TotalDue), 2) AS Total_w_tax
  FROM
    adwentureworks_db.salesorderheader AS salesorder
  LEFT JOIN
    adwentureworks_db.salesterritory AS salesterritory
  ON
    salesterritory.TerritoryID = salesorder.TerritoryID
  LEFT JOIN
    adwentureworks_db.salesperson AS salesperson
  ON
    salesperson.SalesPersonID = salesorder.SalesPersonID
  GROUP BY
    LAST_DAY(DATE(salesorder.OrderDate)),
    salesterritory.CountryRegionCode,
    salesterritory.name )

SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  ROUND(SUM(Total_w_tax) OVER(PARTITION BY CountryRegionCode, Region ORDER BY order_month), 2) AS Cumulative_Sum
FROM
  Sales;

# task 2.3



WITH
  Sales AS (
  SELECT
    LAST_DAY(DATE(OrderDate)) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorder.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorder.CustomerID) AS number_customers,
    COUNT(DISTINCT salesperson.SalesPersonID) AS no_salesPersons,
    ROUND(SUM(salesorder.TotalDue), 2) AS Total_w_tax
  FROM
    adwentureworks_db.salesorderheader AS salesorder
  LEFT JOIN
    adwentureworks_db.salesterritory AS salesterritory
  ON
    salesterritory.TerritoryID = salesorder.TerritoryID
  LEFT JOIN
    adwentureworks_db.salesperson AS salesperson
  ON
    salesperson.SalesPersonID = salesorder.SalesPersonID
  GROUP BY
    LAST_DAY(DATE(salesorder.OrderDate)),
    salesterritory.CountryRegionCode,
    salesterritory.name )

SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  RANK() OVER (PARTITION BY Region ORDER BY Total_w_tax DESC) AS Country_Sales_Rank,
  ROUND(SUM(Total_w_tax) OVER(PARTITION BY CountryRegionCode, Region ORDER BY order_month), 2) AS Cumulative_Sum
FROM
  Sales
ORDER BY Total_w_tax DESC;

# task 2.4


WITH
  Sales AS (
  SELECT
    LAST_DAY(DATE(OrderDate)) AS order_month,
    salesterritory.CountryRegionCode AS CountryRegionCode,
    salesterritory.Name AS Region,
    COUNT(salesorder.SalesOrderID) AS number_orders,
    COUNT(DISTINCT salesorder.CustomerID) AS number_customers,
    COUNT(DISTINCT salesperson.SalesPersonID) AS no_salesPersons,
    ROUND(SUM(salesorder.TotalDue), 2) AS Total_w_tax,
    ROUND(AVG((salesorder.TaxAmt/salesorder.TotalDue)*100),1) AS Avg_tax_rate,
    (COUNT(CASE WHEN salesorder.TaxAmt IS NOT NULL 
               THEN salesorder.TerritoryID END)) AS provinces_with_tax
  FROM
    adwentureworks_db.salesorderheader AS salesorder
  LEFT JOIN
    adwentureworks_db.salesterritory AS salesterritory
  ON
    salesterritory.TerritoryID = salesorder.TerritoryID
  LEFT JOIN
    adwentureworks_db.salesperson AS salesperson
  ON
    salesperson.SalesPersonID = salesorder.SalesPersonID
  GROUP BY
    LAST_DAY(DATE(salesorder.OrderDate)),
    salesterritory.CountryRegionCode,
    salesterritory.name )

SELECT
  order_month,
  CountryRegionCode,
  Region,
  number_orders,
  number_customers,
  no_salesPersons,
  Total_w_tax,
  RANK() OVER (PARTITION BY Region ORDER BY Total_w_tax DESC) AS Country_Sales_Rank,
  ROUND(SUM(Total_w_tax) OVER(PARTITION BY CountryRegionCode, Region ORDER BY order_month), 2) AS Cumulative_Sum,
  Avg_tax_rate,
  ROUND(provinces_with_tax / COUNT(*) OVER (PARTITION BY CountryRegionCode), 2) AS perc_provinces_w_tax
 
FROM
  Sales
ORDER BY Total_w_tax DESC;
