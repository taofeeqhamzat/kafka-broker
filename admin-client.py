from confluent_kafka.admin import AdminClient

# Configure the AdminClient with your Kafka broker(s)
admin_config = {'bootstrap.servers': 'localhost:9092'}
admin = AdminClient(admin_config)

try:
    # List all consumer groups with a timeout
    groups = admin.list_groups(timeout=10)

    print(f"{len(groups)} consumer groups found:")
    for g in groups:
        print(f'  Group: "{g.id}" with {len(g.members)} member(s), protocol: {g.protocol}, protocol_type: {g.protocol_type}')
        for m in g.members:
            print(f'    Member ID: {m.id}, Client ID: {m.client_id}, Client Host: {m.client_host}')

except Exception as e:
    print(f"Error listing consumer groups: {e}")
