import geoip2.database
from user_agents import parse
geocity_data_path="/home/ec2-user/GeoLiteCity/GeoLite2-City.mmdb"

def getDeviceInfo(ua_string):
	user_agent = parse(ua_string)
	browser   = user_agent.browser.family
	browser_version = user_agent.browser.version_string
	os = user_agent.os.family
	os_version = user_agent.os.version_string
	device = user_agent.device.family
        device_type = ""
	if (user_agent.is_mobile):
		device_type= "Mobile"
	elif(user_agent.is_tablet) :
		device_type= "Tablet"
	elif(user_agent.is_pc) :
		device_type= "Desktop"
	else:	
		device_type= "Others"
	return (browser,browser_version,os,os_version,device,device_type)

def getLocationInfo(ip):
	reader = geoip2.database.Reader(geocity_data_path)
	response = reader.city(ip)
	region = response.subdivisions.most_specific.name
	city = response.city.name
	country = response.country.name
	latitude = response.location.latitude
	longitude = response.location.longitude
	reader.close()
	return (region,city,country,latitude,longitude)


