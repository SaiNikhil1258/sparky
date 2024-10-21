# # import csv
# # import random
# # import datetime
# # from datetime import timedelta
# #
# # # Define the headers
# # headers = ["id", "product_id", "transaction_id", "price", "customer_id", "date"]
# #
# # # Define your data (each sublist corresponds to a row)
# # def random_date():
# #     start_date = datetime.date(2023, 1, 1)
# #     end_date = datetime.date(2023, 12, 31)
# #     delta = end_date - start_date
# #     random_days = random.randint(0, delta.days)
# #     return start_date + timedelta(days=random_days)
# #
# # # Generate sales data
# # sales_data = [
# #     (
# #         i,
# #         f"prod{random.randint(1, 50)}",
# #         f"trans{i}",
# #         random.uniform(10, 500),
# #         f"cust{random.randint(1, 100)}",
# #         random_date()
# #     )
# #     for i in range(100000)
# # ]# Write to a CSV file
# # with open('output_file.csv', mode='w', newline='') as file:
# #     writer = csv.writer(file)
# #
# #     # Write the header
# #     writer.writerow(headers)
# #
# #     # Write the data
# #     writer.writerows(sales_data)
# #
# # print("Data written to output_file.csv successfully.")
# import json
# import random
# import string
# from datetime import datetime, timedelta
#
# # function to generate random strings
# def random_string(length):
#     return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
#
# # function to generate random datetime
# def random_date(start, end):
#     return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
#
# # function to generate a random boolean
# def random_bool():
#     return random.choice(["true", "false"])
#
# # function to generate complex nested json data
# def generate_json_data(num_records):
#     data = []
#     start_date = datetime(2018, 1, 1)
#     mid__date = datetime(2018, 2, 1)
#     end_date = datetime(2018, 2, 28)
#
#     for _ in range(num_records):
#         record = {
#             "order": {
#                 "order_id": random_string(10),
#                 "order_date": random_date(start_date, mid__date).strftime("%y-%m-%d"),
#                 "customer": {
#                     "user_id": random_string(8),
#                     "name": {
#                         "first_name": random_string(5),
#                         "last_name": random_string(7)
#                     },
#                     "email": f"{random_string(5)}@example.com",
#                     "account_balance": round(random.uniform(1000, 10000), 2),
#                     "loyalty": {
#                         "loyalty_points": random.randint(100, 1000),
#                         "tier": random.choice(["bronze", "silver", "gold", "platinum"])
#                     }
#                 },
#                 "shipping": {
#                     "address": {
#                         "city": random.choice(["new york", "san francisco", "austin", "seattle"]),
#                         "street_address": f"{random.randint(100, 999)} {random_string(10)} street",
#                         "zip_code": random_string(5),
#                         "coordinates": {
#                             "lat": round(random.uniform(-90, 90), 6),
#                             "lon": round(random.uniform(-180, 180), 6)
#                         }
#                     },
#                     "estimated_delivery": random_date(mid__date, end_date).strftime("%y-%m-%d"),
#                     "shipping_cost": round(random.uniform(5, 50), 2),
#                     "tracking_number": random_string(12),
#                     "shipped": random_bool()
#                 },
#                 "payment": {
#                     "method": random.choice(["credit card", "paypal", "bank transfer"]),
#                     "amount": round(random.uniform(50, 5000), 2),
#                     "discount": round(random.uniform(5, 100), 2),
#                 },
#                 "items": [
#                     {
#                         "product_id": random_string(8),
#                         "name": random_string(12),
#                         "category": random.choice(["electronics", "clothing", "books", "home"]),
#                         "price": round(random.uniform(10, 500), 2),
#                         "quantity": random.randint(1, 10)
#                     } for _ in range(random.randint(1, 5))
#                 ]
#             }
#         }
#         data.append(record)
#
#     return data
#
# # generate 1000 complex json records
# large_dataset = generate_json_data(10000)
#
# # save the data to a json file
# json_data = json.dumps(large_dataset, indent=4)
# json_file_path = "Ecommerce_data.json"
#
# with open(json_file_path, "w") as json_file:
#     json_file.write(json_data)
#
# print("done")
#
#
#


