# AWS_Snapshots
Creates EBS snapshots of volumes that are tagged.  Simply add the following tags to your EC2 instances and snapshots will be created and retained.

1) backup:  daily, weekly, monthly
2) retention: number of days to retain

# Create lambda job to run on a schedule
I won't get into the specifics of this.  Just create a new lambda job that runs at a scheduled time.  This python script handles the rest.
