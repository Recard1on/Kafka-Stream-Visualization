import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# Set kafka server and topic
bootstrap_servers = 'localhost:9092' 
topic_name = 'wikipedia-events'  

# create kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

domain_counts = {}

# This value represents the maximum value to be included in the "Other" class.
threshold = 50

# Create Graphic
fig, (ax1, ax2) = plt.subplots(1, 2)

# Graphic configuration
ax1.set_title("Domain Numbers")
ax2.set_title("Bot Numbers")

# Values of start
labels1 = []
sizes1 = []
labels2 = ['Bot', 'Non-Bot']
sizes2 = [0, 0]

data_count = 0

# Update graphic 
def update(frame):
    global domain_counts, data_count, sizes2
    parsed_json = next(consumer).value
    domain = parsed_json["meta"]["domain"]
    is_bot = parsed_json.get("bot", False)
    truncated_domain = domain[:13]  # We have long domain name. So shorten them and prevent them from getting tangled.
    
    # If this domain name exists, increase it, if not, create a new domain name.
    if truncated_domain in domain_counts:
        domain_counts[truncated_domain] += 1
    else:
        domain_counts[truncated_domain] = 1
    
    # Let's collect the numbers below the threshold under the "Other" category.
    domain_counts_grouped = {}
    other_count = 0
    for key, value in domain_counts.items():
        if value >= threshold:
            domain_counts_grouped[key] = value
        else:
            other_count += value
    if other_count > 0:
        domain_counts_grouped["Other"] = other_count
    
    # Update graphic 1's data
    labels1 = list(domain_counts_grouped.keys())
    sizes1 = list(domain_counts_grouped.values())
    
    # Update graphic 2's data
    if is_bot:
        sizes2[0] += 1
    else:
        sizes2[1] += 1
    
    # Update graphic 1's data
    ax1.clear()
    ax1.set_title("Domain Numbers")
    ax1.pie(sizes1, labels=labels1, autopct='%1.1f%%', startangle=140)
    
    # Update graphic 2's data
    ax2.clear()
    ax2.set_title("Bot Numbers")
    ax2.pie(sizes2, labels=labels2, autopct='%1.1f%%', startangle=140)
    
    # Add total data count 
    data_count += 1
    ax1.annotate(f"Total Data Count: {data_count}", (1, -0.12), xycoords="axes fraction", ha="center")

# Create animation
ani = FuncAnimation(fig, update, interval=40)  # Data is fast. That's why we choose the number that is smaller than the range value. Data will be updated every 40 ms

# Show graphics
plt.show()
