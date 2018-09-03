import boto3
import collections
import datetime
import re

ec = boto3.client('ec2')
ec2 = boto3.resource('ec2')
iam = boto3.client('iam')

def lambda_handler(event, context):
    today = datetime.datetime.today().day
    dayofweek = datetime.datetime.today().weekday()
    
    daily = ec.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['backup']},
            {'Name': 'tag-value', 'Values': ['daily']},
        ]
    ).get(
        'Reservations', []
    )

    weekly = ec.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['backup']},
            {'Name': 'tag-value', 'Values': ['weekly']},
        ]
    ).get(
        'Reservations', []
    )

    monthly = ec.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['backup']},
            {'Name': 'tag-value', 'Values': ['monthly']},
        ]
    ).get(
        'Reservations', []
    )

    reservations = daily
    if today == 1:
        reservations = reservations + monthly
    if dayofweek == 6:
        reservations = reservations + weekly
    instances = [
        i for r in reservations
        for i in r['Instances']
    ]
    
    print "Found %d instances that need backing up" % len(instances)
    to_tag = collections.defaultdict(list)
    
    for instance in instances:
        try:
            retention_days = [
                int(t.get('Value')) for t in instance['Tags']
                if t['Key'] == 'retention'][0]
        except IndexError:
            retention_days = 7
        for dev in instance['BlockDeviceMappings']:
            if dev.get('Ebs', None) is None:
                continue
            vol_id = dev['Ebs']['VolumeId']
            vol = ec2.Volume(id=vol_id)

            print "Found EBS volume %s on instance %s" % (
                vol_id, instance['InstanceId'])

            snap = ec.create_snapshot(
                VolumeId=vol_id,
            )

			newSnapTags = []
            name = None

			#Add tags from orginial volume and remove any problematic 'aws:*' tags
            for tag in vol.tags:
                if tag['Key'] == 'Name':
                    name = tag.get('Value')
				if ('aws:' not in tag['Key']):
                    newSnapTags.append(tag)

			#append house-keeping tags
			newSnapTags.append({{'Key': 'parentinstance', 'Value': instance['InstanceId']}})
			newSnapTags.append({'Key': 'Name', 'Value': name})
			newSnapTags.append({'Key': 'mountpoint', 'Value': dev['DeviceName']})

            ec.create_tags(
                Resources=[snap['SnapshotId']],
                Tags=newSnapTags
			)

            to_tag[retention_days].append(snap['SnapshotId'])

            print "Retaining snapshot %s of volume %s from instance %s for %d days" % (
                snap['SnapshotId'],
                vol_id,
                instance['InstanceId'],
                retention_days,
            )

    for retention_days in to_tag.keys():
        delete_date = datetime.date.today() + datetime.timedelta(days=retention_days)
        delete_fmt = delete_date.strftime('%Y-%m-%d')
        print "Will delete %d snapshots on %s" % (len(to_tag[retention_days]), delete_fmt)
        ec.create_tags(
            Resources=to_tag[retention_days],
            Tags=[
                {'Key': 'deleteon', 'Value': delete_fmt},
            ]
        )
    #Delete expired snapshots
    account_ids = list()
    
    try:
        iam.get_user()
    except Exception as e:
        account_ids.append(re.search(r'(arn:aws:sts::)([0-9]+)', str(e)).groups()[1])
    delete_on = datetime.date.today().strftime('%Y-%m-%d')
    filters = [
        {'Name': 'tag-key', 'Values': ['deleteon']},
        {'Name': 'tag-value', 'Values': [delete_on]},
    ]
    snapshot_response = ec.describe_snapshots(OwnerIds=account_ids, Filters=filters)

    for snap in snapshot_response['Snapshots']:
        print "Deleting snapshot %s" % snap['SnapshotId']
        ec.delete_snapshot(SnapshotId=snap['SnapshotId'])
