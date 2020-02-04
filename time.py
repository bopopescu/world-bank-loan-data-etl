from datetime import datetime, timedelta
import sys

start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')

for x in range((end_date - start_date).days + 1):
    next_date = start_date + timedelta(x)
    print ("%s,%s,%s" % (next_date.strftime('%B'), next_date.strftime('%Y'), next_date.strftime('%Y_%m_%d')))