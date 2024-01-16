import geocoder

location = geocoder.ip('me')
print(location.latlng)
