import random
import csv
from datetime import datetime, timedelta

def generate_row(transaction_no):
	# Random product names and categories
	products = [
		("Treadmill", "Cardio"),
		("Dumbbell", "Strength"),
		("Yoga Mat", "Accessories"),
		("Exercise Bike", "Cardio"),
		("Kettlebell", "Strength"),
		("Resistance Band", "Accessories"),
		("Rowing Machine", "Cardio"),
		("Barbell", "Strength"),
		("Pull-up Bar", "Accessories"),
		("Elliptical", "Cardio"),
	]

	product_name, category = random.choice(products)

	# Random price based on product category
	price = round(random.uniform(50, 1500), 2)

	# Random payment mode
	payment_modes = ["Credit Card", "Debit Card", "Cash", "Online Payment"]
	payment_mode = random.choice(payment_modes)

	# Random customer age
	customer_age = random.choices(
		range(18, 65),
		weights=[0.05] * 5 + [0.1] * 10 + [0.2] * 10 + [0.1] * 10 + [0.05] * 12,
		k=1
	)[0]

	# Random customer gender
	customer_gender = random.choice(["Male", "Female", "Other"])

	# Random date within the last year
	start_date = datetime.now() - timedelta(days=365)
	random_days = random.randint(0, 365)
	date = (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')

	return {
		"transactionNo": transaction_no,
		"date": date,
		"price": price,
		"product_name": product_name,
		"category": category,
		"payment_mode": payment_mode,
		"customer_age": customer_age,
		"customer_gender": customer_gender
	}

# Generate 10,000 rows
data = [generate_row(i) for i in range(1, 10001)]

# Write data to CSV file
with open('gym_equipment_sales.csv', 'w', newline='') as csvfile:
	fieldnames = ["transactionNo", "date", "price", "product_name", "category", "payment_mode", "customer_age", "customer_gender"]
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

	writer.writeheader()
	writer.writerows(data)

print("Data written to gym_equipment_sales.csv")