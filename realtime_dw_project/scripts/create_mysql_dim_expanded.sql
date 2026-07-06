CREATE DATABASE IF NOT EXISTS ecommerce DEFAULT CHARACTER SET utf8mb4;
USE ecommerce;

DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_shop;
DROP TABLE IF EXISTS dim_region;

-- ============================================================
-- dim_region: 20 个地区（20 个不同城市）
-- ============================================================
CREATE TABLE dim_region (
  region_id BIGINT PRIMARY KEY,
  province VARCHAR(50) NOT NULL,
  city VARCHAR(50) NOT NULL
);

INSERT INTO dim_region VALUES
(1,  'Guangdong',     'Shenzhen'),
(2,  'Zhejiang',      'Hangzhou'),
(3,  'Shanghai',      'Shanghai'),
(4,  'Beijing',       'Beijing'),
(5,  'Sichuan',       'Chengdu'),
(6,  'Guangdong',     'Guangzhou'),
(7,  'Hubei',         'Wuhan'),
(8,  'Jiangsu',       'Nanjing'),
(9,  'Fujian',        'Xiamen'),
(10, 'Shandong',      'Qingdao'),
(11, 'Shaanxi',       'Xi''an'),
(12, 'Hunan',         'Changsha'),
(13, 'Liaoning',      'Dalian'),
(14, 'Chongqing',     'Chongqing'),
(15, 'Henan',         'Zhengzhou'),
(16, 'Tianjin',       'Tianjin'),
(17, 'Anhui',         'Hefei'),
(18, 'Guizhou',       'Guiyang'),
(19, 'Yunnan',        'Kunming'),
(20, 'Guangxi',       'Nanning');

-- ============================================================
-- dim_shop: 20 个店铺，分布在 20 个地区
-- ============================================================
CREATE TABLE dim_shop (
  shop_id BIGINT PRIMARY KEY,
  shop_name VARCHAR(100) NOT NULL,
  region_id BIGINT NOT NULL
);

INSERT INTO dim_shop VALUES
(1,  'Digital Flagship Store',     1),
(2,  'Fashion Trend Store',        2),
(3,  'Home Living Store',          3),
(4,  'Sports Power Store',         4),
(5,  'Beauty Glow Store',          5),
(6,  'Food Paradise Store',        6),
(7,  'Mother Baby Care Store',     7),
(8,  'Book Wisdom Store',          8),
(9,  'Car Accessories Store',      9),
(10, 'Health Wellness Store',      10),
(11, 'Digital Zone Store',         11),
(12, 'Fashion Weekly Store',       12),
(13, 'Home Comfort Store',         13),
(14, 'Sports Unlimited Store',     14),
(15, 'Beauty Nature Store',        15),
(16, 'Food Fresh Store',           16),
(17, 'Baby Happy Store',           17),
(18, 'Book Universe Store',        18),
(19, 'Car Tech Store',             19),
(20, 'Health Life Store',          20);

-- ============================================================
-- dim_product: 100 个商品，10 个品类 × 10 个商品
-- product_id: 1001 ~ 1100
-- ============================================================
CREATE TABLE dim_product (
  product_id BIGINT PRIMARY KEY,
  product_name VARCHAR(100) NOT NULL,
  category_id BIGINT NOT NULL,
  category_name VARCHAR(100) NOT NULL,
  price DECIMAL(10,2) NOT NULL
);

-- Category 10: Digital 数码 (price 500-5000)
INSERT INTO dim_product VALUES
(1001, 'Wireless Noise-Cancelling Earbuds',      10, 'Digital',  899.00),
(1002, 'Mechanical Gaming Keyboard RGB',          10, 'Digital',  599.00),
(1003, '4K Ultra HD Monitor 27inch',              10, 'Digital', 2499.00),
(1004, 'Portable Bluetooth Speaker',              10, 'Digital',  699.00),
(1005, 'USB-C Hub 12-in-1',                       10, 'Digital',  549.00),
(1006, 'Wireless Gaming Mouse',                   10, 'Digital',  799.00),
(1007, 'External SSD 2TB',                        10, 'Digital', 1299.00),
(1008, 'Tablet 11inch WiFi 256GB',                10, 'Digital', 3499.00),
(1009, 'Smartwatch Pro GPS',                      10, 'Digital', 1999.00),
(1010, 'Webcam 1080P Auto Focus',                 10, 'Digital',  599.00);

-- Category 20: Fashion 服饰 (price 50-500)
INSERT INTO dim_product VALUES
(1011, 'Casual Denim Jacket',                     20, 'Fashion',  299.00),
(1012, 'Cotton Linen Shirt',                      20, 'Fashion',  159.00),
(1013, 'Slim Fit Chinos Pants',                   20, 'Fashion',  229.00),
(1014, 'Wool Blend Overcoat',                     20, 'Fashion',  499.00),
(1015, 'Graphic Print T-Shirt',                   20, 'Fashion',   89.00),
(1016, 'High-Waist Wide Leg Jeans',               20, 'Fashion',  259.00),
(1017, 'Silk Blend Scarf',                        20, 'Fashion',  129.00),
(1018, 'Leather Belt Classic',                    20, 'Fashion',  169.00),
(1019, 'Cashmere Sweater',                        20, 'Fashion',  459.00),
(1020, 'Sports Cap Adjustable',                   20, 'Fashion',   69.00);

-- Category 30: Sports 运动 (price 30-800)
INSERT INTO dim_product VALUES
(1021, 'Professional Running Shoes',              30, 'Sports',   599.00),
(1022, 'Yoga Mat Non-Slip 6mm',                   30, 'Sports',    89.00),
(1023, 'Adjustable Dumbbell Set 20kg',            30, 'Sports',   399.00),
(1024, 'Basketball Indoor Outdoor',               30, 'Sports',   199.00),
(1025, 'Resistance Bands Set 5pcs',               30, 'Sports',    49.00),
(1026, 'Treadmill Foldable Home',                 30, 'Sports',   799.00),
(1027, 'Swimming Goggles Anti Fog',               30, 'Sports',    79.00),
(1028, 'Cycling Helmet Lightweight',              30, 'Sports',   259.00),
(1029, 'Jump Rope Speed Cable',                   30, 'Sports',    39.00),
(1030, 'Tennis Racket Professional',              30, 'Sports',   349.00);

-- Category 40: Home 家居 (price 20-600)
INSERT INTO dim_product VALUES
(1031, 'Memory Foam Pillow',                      40, 'Home',     129.00),
(1032, 'Stainless Steel Thermos 500ml',           40, 'Home',      89.00),
(1033, 'LED Desk Lamp Eye Care',                  40, 'Home',     199.00),
(1034, 'Non-Stick Frying Pan 28cm',               40, 'Home',     159.00),
(1035, 'Robot Vacuum Cleaner',                    40, 'Home',     599.00),
(1036, 'Aroma Diffuser Ultrasonic',               40, 'Home',     109.00),
(1037, 'Bamboo Storage Shelf',                    40, 'Home',     259.00),
(1038, 'Blackout Curtains Set',                   40, 'Home',     149.00),
(1039, 'Electric Kettle Temperature Control',     40, 'Home',     179.00),
(1040, 'Cushion Cover Set of 4',                  40, 'Home',      49.00);

-- Category 50: Beauty 美妆 (price 20-300)
INSERT INTO dim_product VALUES
(1041, 'Vitamin C Serum 30ml',                    50, 'Beauty',   129.00),
(1042, 'Hyaluronic Acid Moisturizer',             50, 'Beauty',   159.00),
(1043, 'Sunscreen SPF50+ PA++++',                 50, 'Beauty',    89.00),
(1044, 'Retinol Night Cream 50g',                 50, 'Beauty',   229.00),
(1045, 'Lipstick Matte 12 Colors',                50, 'Beauty',    79.00),
(1046, 'Eye Cream Anti-Aging 15ml',               50, 'Beauty',   199.00),
(1047, 'Facial Mask Sheet 10pcs',                 50, 'Beauty',    59.00),
(1048, 'Cleansing Oil 200ml',                     50, 'Beauty',   119.00),
(1049, 'Foundation Liquid SPF20',                 50, 'Beauty',   189.00),
(1050, 'Perfume EDP 50ml',                        50, 'Beauty',   299.00);

-- Category 60: Food 食品 (price 5-100)
INSERT INTO dim_product VALUES
(1051, 'Organic Green Tea 250g',                  60, 'Food',      59.00),
(1052, 'Mixed Nuts Premium 500g',                 60, 'Food',      89.00),
(1053, 'Imported Dark Chocolate 200g',            60, 'Food',      69.00),
(1054, 'Instant Oatmeal Pack 1kg',                60, 'Food',      39.00),
(1055, 'Dried Mango Slices 300g',                 60, 'Food',      35.00),
(1056, 'Freeze-Dried Coffee 100g',                60, 'Food',      79.00),
(1057, 'Protein Bar Box of 12',                   60, 'Food',      99.00),
(1058, 'Sesame Paste Pure 350g',                  60, 'Food',      29.00),
(1059, 'Honey Wildflower 500g',                   60, 'Food',      59.00),
(1060, 'Rice Cracker Assorted 400g',              60, 'Food',      25.00);

-- Category 70: MotherBaby 母婴 (price 30-500)
INSERT INTO dim_product VALUES
(1061, 'Baby Diapers XL 60pcs',                   70, 'MotherBaby', 149.00),
(1062, 'Infant Formula Stage 3 800g',             70, 'MotherBaby', 299.00),
(1063, 'Baby Stroller Lightweight',               70, 'MotherBaby', 499.00),
(1064, 'Baby Wipes Unscented 3pk',                70, 'MotherBaby',  59.00),
(1065, 'Baby Feeding Bottle Set',                 70, 'MotherBaby', 129.00),
(1066, 'Baby Safety Gate',                        70, 'MotherBaby', 259.00),
(1067, 'Maternity Pillow U-Shaped',               70, 'MotherBaby', 199.00),
(1068, 'Baby Monitor Camera WiFi',                70, 'MotherBaby', 399.00),
(1069, 'Electric Breast Pump',                    70, 'MotherBaby', 349.00),
(1070, 'Baby Bath Tub Foldable',                  70, 'MotherBaby',  89.00);

-- Category 80: Book 图书 (price 10-100)
INSERT INTO dim_product VALUES
(1071, 'Data Intensive Applications',             80, 'Book',      89.00),
(1072, 'Designing Data-Intensive Apps',           80, 'Book',      79.00),
(1073, 'Flink Fundamentals',                      80, 'Book',      69.00),
(1074, 'Clean Code Paperback',                    80, 'Book',      59.00),
(1075, 'Deep Learning Illustrated',               80, 'Book',      99.00),
(1076, 'Kafka The Definitive Guide',              80, 'Book',      69.00),
(1077, 'Python Cookbook 3rd Edition',             80, 'Book',      79.00),
(1078, 'SQL Performance Explained',               80, 'Book',      49.00),
(1079, 'The Art of Data Engineering',             80, 'Book',      89.00),
(1080, 'Distributed Systems Notes',               80, 'Book',      39.00);

-- Category 90: Car 汽车用品 (price 50-2000)
INSERT INTO dim_product VALUES
(1081, 'Dash Cam 4K WiFi',                        90, 'Car',      899.00),
(1082, 'Car Phone Holder Magnetic',               90, 'Car',       69.00),
(1083, 'Car Air Purifier USB',                    90, 'Car',      299.00),
(1084, 'Tire Inflator Portable',                  90, 'Car',      199.00),
(1085, 'Car Seat Cover Leather Set',              90, 'Car',      599.00),
(1086, 'Jump Starter Power Bank',                 90, 'Car',      399.00),
(1087, 'Car Floor Mats Waterproof',               90, 'Car',      259.00),
(1088, 'Bluetooth Car Adapter FM',                90, 'Car',       89.00),
(1089, 'Car Vacuum Cleaner Wireless',             90, 'Car',      349.00),
(1090, 'Roof Rack Cross Bars Universal',          90, 'Car',     1899.00);

-- Category 100: Health 健康 (price 20-300)
INSERT INTO dim_product VALUES
(1091, 'Vitamin D3 Supplement 200tabs',           100, 'Health',    89.00),
(1092, 'Fish Oil Omega-3 120caps',                100, 'Health',   129.00),
(1093, 'Digital Blood Pressure Monitor',          100, 'Health',   299.00),
(1094, 'Probiotics Capsules 60caps',              100, 'Health',   159.00),
(1095, 'Forehead Thermometer Infrared',           100, 'Health',    79.00),
(1096, 'Magnesium Complex 90tabs',                100, 'Health',    99.00),
(1097, 'Electric Toothbrush Sonic',               100, 'Health',   259.00),
(1098, 'Sleep Aid Melatonin Gummies',             100, 'Health',    69.00),
(1099, 'Collagen Peptides Powder 300g',           100, 'Health',   199.00),
(1100, 'First Aid Kit Home 100pcs',               100, 'Health',    59.00);
