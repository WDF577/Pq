CREATE DATABASE IF NOT EXISTS ecommerce DEFAULT CHARACTER SET utf8mb4;
USE ecommerce;

DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_shop;
DROP TABLE IF EXISTS dim_region;

CREATE TABLE dim_product (
  product_id BIGINT PRIMARY KEY,
  product_name VARCHAR(100) NOT NULL,
  category_id BIGINT NOT NULL,
  category_name VARCHAR(100) NOT NULL,
  price DECIMAL(10,2) NOT NULL
);

CREATE TABLE dim_shop (
  shop_id BIGINT PRIMARY KEY,
  shop_name VARCHAR(100) NOT NULL,
  region_id BIGINT NOT NULL
);

CREATE TABLE dim_region (
  region_id BIGINT PRIMARY KEY,
  province VARCHAR(50) NOT NULL,
  city VARCHAR(50) NOT NULL
);

INSERT INTO dim_region VALUES
(1, 'Guangdong', 'Shenzhen'),
(2, 'Zhejiang', 'Hangzhou'),
(3, 'Shanghai', 'Shanghai'),
(4, 'Beijing', 'Beijing'),
(5, 'Sichuan', 'Chengdu');

INSERT INTO dim_shop VALUES
(1, 'Digital Store', 1),
(2, 'Fashion Store', 2),
(3, 'Home Store', 3),
(4, 'Sports Store', 4),
(5, 'Beauty Store', 5);

INSERT INTO dim_product VALUES
(1001, 'Wireless Earphones', 10, 'Digital', 199.00),
(1002, 'Mechanical Keyboard', 10, 'Digital', 299.00),
(1003, 'Casual Jacket', 20, 'Fashion', 159.00),
(1004, 'Running Shoes', 30, 'Sports', 399.00),
(1005, 'Thermos Cup', 40, 'Home', 69.00),
(1006, 'Skincare Set', 50, 'Beauty', 259.00);
