#!/usr/bin/python

all_ads = {}
all = open('ad_all.out','r')
for line in all:
	ad_id = int(line)
	if ad_id in all_ads:
		all_ads[ad_id] = all_ads[ad_id]+1
	else:
		all_ads[ad_id] = 1

bad_ads = {}
bad = open('ad_bad.out','r')
for line in bad:
	bad_ad_id = int(line)
	if bad_ad_id in bad_ads:
		bad_ads[bad_ad_id] = bad_ads[bad_ad_id]+1
	else:
		bad_ads[bad_ad_id] = 1

if len(all_ads) <= len(bad_ads):
				print "Some missing primary keys!"

for b_ad,b_count in bad_ads.iteritems():
				a_count = all_ads[b_ad]
				if b_count != a_count-1:
								print("Id %d has incorrect bad count!. Should have %d, but actually has %d"%(b_ad,a_count-1,b_count))

