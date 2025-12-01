---------------------------------------------------->1.𝐅𝐢𝐧𝐝 𝐭𝐡𝐞 𝐅𝐢𝐫𝐬𝐭 𝐏𝐮𝐫𝐜𝐡𝐚𝐬𝐞 𝐌𝐚𝐝𝐞 𝐛𝐲 𝐄𝐯𝐞𝐫𝐲 𝐂𝐮𝐬𝐭𝐨𝐦𝐞𝐫<---------------------------------------------------------------

---->Table Creation<-----
CREATE TABLE purchase (
  customer_id BIGINT,
  purchase_date DATE,
  amount DECIMAL(10,2),
  transaction_id BIGINT
)

---->Insert_Values<-----
 INSERT INTO purchase (customer_id, purchase_date, amount, transaction_id) VALUES
  (101, '2024-06-01', 49.99, 1001),
  (101, '2024-06-04', 49.99, 1002),
  (102, '2024-06-02', 19.95, 1001),
  (102, '2024-06-08', 19.95, 1003),
  (102, '2024-07-02', 19.95, 1002),
  (103, '2024-06-03', 5.00, 1001),
  (104, '2024-06-04', 15.75, 1001),
  (104, '2024-06-08', 15.75, 1005),
  (105, '2024-06-05', 120.00, 1001),
  (105, '2024-06-09', 120.00, 1006),
  (105, '2024-06-15', 120.00, 1007),
  (106, '2024-06-06', 8.99, 1001);


----->Query<-----


with cte as(
  select customer_id,purchase_date,amount,transaction_id,
  row_number() over(partition by customer_id order by purchase_date asc, transaction_id asc) as rnk
  from purchase
)

select customer_id,
purchase_date as first_purchase_date,
amount,
transaction_id

 from cte where rnk=1



---->Pyspark_code<-----



