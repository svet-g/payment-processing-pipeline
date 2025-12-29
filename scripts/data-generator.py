import csv
import random
from datetime import datetime, timedelta

def generate_swift_data(num_records, output_file, anomaly_rate=0.05):
    countries = ['US', 'UK', 'DE', 'FR', 'JP', 'CN', 'AU', 'CA', 'BR', 'IN']
    banks = ['BOFAUS3NXXX', 'CHASUS33XXX', 'DEUTDEFFXXX', 'BARCGB22XXX', 'BNPAFRPPXXX']
    currencies = ['USD', 'EUR', 'GBP', 'JPY']
    msg_types = ['MT103', 'MT202', 'MT950']

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'sender_bic', 'receiver_bic', 'amount', 'currency', 'msg_type'])
        
        start_date = datetime.now() - timedelta(days=30)
        for _ in range(num_records):
            if random.random() < anomaly_rate:
                # Generate an anomaly
                anomaly_type = random.choice(['high_amount', 'invalid_bic', 'future_date', 'duplicate', 'wrong_currency'])
                row = generate_anomaly(anomaly_type, start_date, banks, currencies, msg_types)
            else:
                # Generate normal data
                row = generate_normal_data(start_date, banks, currencies, msg_types)
            
            writer.writerow(row)

def generate_normal_data(start_date, banks, currencies, msg_types):
    timestamp = start_date + timedelta(minutes=random.randint(1, 43200))
    sender_bic = random.choice(banks)
    receiver_bic = random.choice(banks)
    amount = round(random.uniform(100, 1000000), 2)
    currency = random.choice(currencies)
    msg_type = random.choice(msg_types)
    return [timestamp, sender_bic, receiver_bic, amount, currency, msg_type]

def generate_anomaly(anomaly_type, start_date, banks, currencies, msg_types):
    if anomaly_type == 'high_amount':
        return generate_high_amount_anomaly(start_date, banks, currencies, msg_types)
    elif anomaly_type == 'invalid_bic':
        return generate_invalid_bic_anomaly(start_date, banks, currencies, msg_types)
    elif anomaly_type == 'future_date':
        return generate_future_date_anomaly(start_date, banks, currencies, msg_types)
    elif anomaly_type == 'duplicate':
        return generate_duplicate_anomaly(start_date, banks, currencies, msg_types)
    elif anomaly_type == 'wrong_currency':
        return generate_wrong_currency_anomaly(start_date, banks, currencies, msg_types)

def generate_high_amount_anomaly(start_date, banks, currencies, msg_types):
    row = generate_normal_data(start_date, banks, currencies, msg_types)
    row[3] = round(random.uniform(10000000, 100000000), 2)  # Very high amount
    return row

def generate_invalid_bic_anomaly(start_date, banks, currencies, msg_types):
    row = generate_normal_data(start_date, banks, currencies, msg_types)
    row[1] = 'INVALID' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
    return row

def generate_future_date_anomaly(start_date, banks, currencies, msg_types):
    row = generate_normal_data(start_date, banks, currencies, msg_types)
    row[0] = datetime.now() + timedelta(days=random.randint(1, 30))
    return row

def generate_duplicate_anomaly(start_date, banks, currencies, msg_types):
    return generate_normal_data(start_date, banks, currencies, msg_types)

def generate_wrong_currency_anomaly(start_date, banks, currencies, msg_types):
    row = generate_normal_data(start_date, banks, currencies, msg_types)
    row[4] = random.choice(['AUD', 'CAD', 'CHF', 'CNY'])  # Uncommon currencies for SWIFT
    return row

# Generate 10,000 records with 5% anomalies
generate_swift_data(10000, 'swift_messages_with_anomalies.csv', anomaly_rate=0.05)