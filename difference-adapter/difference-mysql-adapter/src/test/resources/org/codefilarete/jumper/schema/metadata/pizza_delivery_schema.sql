CREATE TABLE Address
(
	id      INT AUTO_INCREMENT PRIMARY KEY,
	address VARCHAR(100) NOT NULL,
	zipCode VARCHAR(10)  NOT NULL,
	city    VARCHAR(50)  NOT NULL
);

CREATE TABLE Customer
(
	id              INT AUTO_INCREMENT PRIMARY KEY,
	lastName        VARCHAR(50) NOT NULL,
	firstName       VARCHAR(50) NOT NULL,
	addressId       INT,
	phoneNumber     VARCHAR(15) NOT NULL,
	inscriptionDate DATE        NOT NULL,
	FOREIGN KEY (addressId) REFERENCES Address (id),
	index (lastName)
);

CREATE TABLE Ingredient
(
	id   INT AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL

);

CREATE TABLE Pizza
(
	id          INT AUTO_INCREMENT PRIMARY KEY,
	name        VARCHAR(50)   NOT NULL,
	ingredients VARCHAR(255)  NOT NULL,
	price       DECIMAL(5, 2) NOT NULL,
	unique `constraint` (name)
);

CREATE TABLE Pizza_Ingredient
(
	pizzaId      INT NOT NULL,
	ingredientId INT NOT NULL,
	FOREIGN KEY (pizzaId) REFERENCES Ingredient (id),
	FOREIGN KEY (ingredientId) REFERENCES Pizza (id),
	PRIMARY KEY (pizzaId, ingredientId)
);

CREATE TABLE Command
(
	id         INT AUTO_INCREMENT PRIMARY KEY,
	customerId INT                                                 NOT NULL,
	`date`     DATETIME                                            NOT NULL,
	totalPrice DECIMAL(5, 2)                                       NOT NULL,
	status     ENUM ('todo', 'preparing', 'delivery', 'delivered') NOT NULL DEFAULT 'preparing',
	FOREIGN KEY (customerId) REFERENCES Customer (id)
);

CREATE TABLE CommandDetail
(
	commandId INT NOT NULL,
	pizzaId   INT NOT NULL,
	quantity  INT NOT NULL,
	FOREIGN KEY (commandId) REFERENCES Command (id),
	FOREIGN KEY (pizzaId) REFERENCES Pizza (id),
	PRIMARY KEY (commandId, pizzaId)
);