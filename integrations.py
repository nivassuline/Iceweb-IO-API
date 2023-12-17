

##
# WORK
##


# import requests

# url = "https://a.klaviyo.com/api/profiles/"

# payload = { "data": {
#         "type": "profile",
#         "attributes": {
#             "email": "sarah.mason@klaviyo-demo.com",
#             "phone_number": "+15005550006",
#             "external_id": "63f64a2b-c6bf-40c7-b81f-bed08162edbe",
#             "first_name": "Sarah",
#             "last_name": "Mason",
#             "organization": "Klaviyo",
#             "title": "Engineer",
#             "image": "https://images.pexels.com/photos/3760854/pexels-photo-3760854.jpeg",
#             "location": {
#                 "address1": "89 E 42nd St",
#                 "address2": "1st floor",
#                 "city": "New York",
#                 "country": "United States",
#                 "region": "NY",
#                 "zip": "10017",
#                 "timezone": "America/New_York",
#                 "ip": "127.0.0.1"
#             },
#         }
#     } }
# headers = {
#     "accept": "application/json",
#     "revision": "2023-10-15",
#     "content-type": "application/json",
#     "Authorization": "Klaviyo-API-Key pk_471fb125deb7dce6a318e29880c32a544a"
# }

# response = requests.post(url, json=payload, headers=headers)

# print(response.text)

###
# WORKS
###

# from customerio import CustomerIO, Regions
# cio = CustomerIO('e86d95116ef5e5381a2a', 'a658e094ff18f06d86b4', region=Regions.US)

# cio.identify(id="5", email='customer@example.com', name='Bob', plan='premium', address='test')

###
# WORKS
###

# import mailchimp_marketing as MailchimpMarketing
# from mailchimp_marketing.api_client import ApiClientError

# try:
#   client = MailchimpMarketing.Client()
#   client.set_config({
#     "api_key": "3bf4a01ec35ec863f203bd238e59c5ff-us21",
#     "server": "us21"
#   })

#   response = client.lists.add_list_member("4f8553254d", {"email_address": "Addiddabvhn71@hotmail.com","status": "unsubscribed" , "merge_fields" : {"FNAME" : "nivos", "LNAME" : "adsasd"}})
#   print(response)

# except ApiClientError as error:
#   print("Error: {}".format(error.text))


###
# WORKS
# https://stackoverflow.com/questions/61427133/adding-subscribers-to-a-list-using-mailchimps-api
###


# import requests

# # Set the endpoint URL
# url = "https://api.sendgrid.com/v3/marketing/contacts"

# # Set the headers
# headers = {
#     "Authorization": "Bearer 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",
#     "Content-Type": "application/json",
# }

# # Set the request body
# data = {
#     "contacts": [
#         {
#             "email": "annahamilton@example.org",
#             "first_name" : 'niv',
#             "last_name": 'ass',
#         }
#     ]
# }

# # Send the PUT request
# response = requests.put(url, headers=headers, json=data)

# # Check the response
# if response.status_code == 200:
#     print("Request successful")
# else:
#     print(f"Request failed with status code {response.status_code}")
#     print(response.text)

###
# WORKS
###

# import requests

# url = 'https://api.hubapi.com/crm/v3/objects/contacts'

# headers = {
#     "Content-Type": "application/json",
#     "authorization" : 'Bearer pat-na1-cec38f84-24b3-4a96-a2c2-de093a7b8faa'
# }


# data = {
#   "properties": {
#     "email": "example@hubspot.com",
#     "firstname": "Jane",
#     "lastname": "Doe",
#     "phone": "(555) 555-5555",
#     "company": "HubSpot",
#     "website": "hubspot.com",
#     "lifecyclestage": "marketingqualifiedlead"
#   }
# }


# # Send the POST request
# response = requests.post(url, headers=headers, json=data)

# # Check the response
# if response.status_code == 200:
#     print("Request successful")
#     print(response.json())
# else:
#     print(f"Request failed with status code {response.status_code}")
#     print(response.text)


###
# WORKS
###

# import requests

# url = "https://api.omnisend.com/v3/contacts"

# payload = {
#     "firstName": "niv",
#     "lastName": "assouline",
#     "country": "United States",
#     "state": "California",
#     "city": "Los Angeles",
#     "address": "6940 Calvin Ave",
#     "postalCode": "91335",
#     "gender": "f",
#     "birthdate": "2001-06-27",
#     "identifiers": [
#         {
#             "type": "email",
#             "channels": {
#                 "email": { "status": "subscribed" }
#             },
#             "id": "niv@iceweb.com"
#         }
#     ]
# }
# headers = {
#     "accept": "application/json",
#     "content-type": "application/json",
#     "X-API-KEY": "656df335237faa9cf404cf33-dRP8r7WyHe61JhTOFJ07s9j1g67m5DutDqqoPsPHWoWsp8jEp4"
# }

# response = requests.post(url, json=payload, headers=headers)

# print(response.text)

###
# WORKS
###


# import requests

# headers = {
# 	'Content-Type': 'application/json',
# }

# data = """{"api_key":"4a0e1533-e416-46db-8638-31731b978827",
#         "email_address":"joe@bloggs.com",
#         "fields":{"FirstName":"Joe","LastName":"Bloggs","Birthday":"2020-12-20"},
#         "tags":["vip"],
#         "status":"SUBSCRIBED"
#         }"""

# response = requests.post('https://emailoctopus.com/api/1.6/lists/{listID}/contacts', headers=headers, data=data)

# print(response.text)

###
# WORKS
##

import requests
from customerio import CustomerIO, Regions
import phonenumbers
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from datetime import datetime, timedelta
# from google.auth.transport.requests import Request
# from google.auth.credentials import AnonymousCredentials
# from google.auth.transport.requests import Request
# from google.auth import jwt
# from google.ads.googleads.client import GoogleAdsClient
# import uuid

data = {
    "address": "7013 Van Antwerp Dr",
    "adults_in_household": "2.0",
    "age": "52.0",
    "bebacks": "1",
    "behaviors": "Cat owner, Healthy living, Luxury lifestyle, Pet owner, Travel - cruises, Travel - foreign, Travel - vacation, Truck owner",
    "children_age_ranges": "0 - 3, 7 - 9",
    "children_between_ages0_3": "1.0",
    "children_between_ages10_12": "0.0",
    "children_between_ages13_18": "0.0",
    "children_between_ages4_6": "0.0",
    "children_between_ages7_9": "1.0",
    "children_in_household": "1.0",
    "city": "Cicero",
    "credit_range": "C. 700-749",
    "date": "2023-11-10",
    "dnc": "-",
    "dwelling_type": "Single Family",
    "education": "Completed College",
    "email": "ballrdaaddrtqasdasdtrsffardassoc@gmail.com",
    "ethnic_group": "White",
    "facebook": "https://www.facebook.com/ABALLARD71",
    "first_name": "Aaron",
    "full_name": "Aaron Ballard",
    "gender": "Male",
    "generation": "2. Generation X (1961-1981)",
    "home_heat_type": "Oil",
    "home_owner": "Home Owner",
    "home_price": "240000.0",
    "home_purchased_years_ago": "9.0",
    "home_value": "368700.0",
    "hour": "13:30:55",
    "household_income": "K. $100,000-$149,999",
    "household_net_worth": "-",
    "income_levels": "100K_150",
    "interests": "Basketball, Car interest, Charitable donor, Collects antiques, Cooking, Dieting/weight loss, Fishing, Fitness, Football, Golfing, Gourmet cooking, Healthy living, Home decorators, Home improvements, Music lifestyle, Outdoors, Snow skiing, Walking",
    "is_multilingual": "0.0",
    "is_political_contributor": "0.0",
    "is_voter": "0.0",
    "language": "English",
    "last_name": "Ballard",
    "linked_in": "-",
    "maid": "3CB9B723-4B19-4154-BAB8-BB91B775B70D",
    "maid_os": "ANDROID",
    "marital_status": "Single",
    "mortgage_age": "-",
    "mortgage_amount": "-",
    "mortgage_loan_type": "NA/Unknown",
    "mortgage_refinance_age": "-",
    "mortgage_refinance_amount": "-",
    "mortgage_refinance_type": "NA/Unknown",
    "net_worth": "G. $50,000 - $99,999",
    "new_credit_offered_household": "D. $501 - $1,000",
    "number_of_vehicles_in_household": "1.0",
    "occupation_detail": "Manager",
    "opt_in": "true",
    "opt_in_date": "2017-01-10",
    "opt_in_ip": "66.102.8.150",
    "opt_in_url": "immediatepaycheck.com",
    "owns_amex_card": "0.0",
    "owns_bank_card": "1.0",
    "owns_investment": "1.0",
    "owns_premium_amex_card": "0.0",
    "owns_premium_card": "0.0",
    "owns_stocks_and_bonds": "1.0",
    "people_in_household": "2.0",
    "personality": "Unknown",
    "phone": "4084658181.0",
    "pixel_first_hit_date": "2023-11-10",
    "pixel_last_hit_date": "2023-11-10",
    "political_party": "Republican",
    "premium_income_household": "AA. Unknown",
    "religion": "Protestant",
    "state": "NY",
    "twitter": "-",
    "urbanicity": "3. Suburban",
    "url": "/neosport-2mm-neoprene-sock.html#136=514",
    "veterans_in_household": "0.0",
    "zip": "13039"
}


def format_phone_number(phone_number):
    try:
        # Parse the input phone number
        parsed_number = phonenumbers.parse(str(phone_number), "US")

        # Check if the number is valid

        # Format the phone number in the international format
        formatted_number = phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        return formatted_number.replace(" ", "")[:-1]

    except Exception:
        return "+10000000000"

# def create_customer_match_user_list(client, customer_id):
#     """Creates a Customer Match user list.

#     Args:
#         client: The Google Ads client.
#         customer_id: The ID for the customer that owns the user list.

#     Returns:
#         The string resource name of the newly created user list.
#     """
#     # Creates the UserListService client.
#     user_list_service_client = client.get_service("UserListService")

#     # Creates the user list operation.
#     user_list_operation = client.get_type("UserListOperation")

#     # Creates the new user list.
#     user_list = user_list_operation.create
#     user_list.name = f"Customer Match list #{uuid.uuid4()}"
#     user_list.description = (
#         "A list of customers that originated from email and physical addresses"
#     )
#     # Sets the upload key type to indicate the type of identifier that is used
#     # to add users to the list. This field is immutable and required for a
#     # CREATE operation.
#     user_list.crm_based_user_list.upload_key_type = (
#         client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
#     )
#     # Customer Match user lists can set an unlimited membership life span;
#     # to do so, use the special life span value 10000. Otherwise, membership
#     # life span must be between 0 and 540 days inclusive. See:
#     # https://developers.devsite.corp.google.com/google-ads/api/reference/rpc/latest/UserList#membership_life_span
#     # Sets the membership life span to 30 days.
#     user_list.membership_life_span = 30

#     response = user_list_service_client.mutate_user_lists(
#         customer_id=customer_id, operations=[user_list_operation]
#     )
#     user_list_resource_name = response.results[0].resource_name
#     print(
#         f"User list with resource name '{user_list_resource_name}' was created."
#     )

#     return user_list_resource_name


def get_birthdate_from_age(age):
    try:
        # Convert age to integer
        age = int(float(age))

        # Get the current year
        current_year = datetime.now().year

        # Calculate the birth year
        birth_year = current_year - age

        # Assuming a birthdate on January 1st of the birth year
        birthdate = datetime(birth_year, 1, 1)

        return birthdate.strftime('%Y-%m-%d')
    except Exception:
        birthdate = datetime(1900, 1, 1)
        return birthdate.strftime('%Y-%m-%d')
    

def bayengage_integration(client_secret,public_id,data,list_id=None):
    # Set the endpoint URL
    url = "https://api.bayengage.com/api/v1/customer/batch"

    # Set the headers
    headers = {
        "x-client-secret": client_secret,
        "x-public-id": public_id,
        "Content-Type": "application/json",
    }


    subs = {
    "subscribers": [
        {
        "email": data["email"],
        "source": "api",
        "email_subscription": "active",
        "custom_fields": [{key:value} for key, value in data.items()],
        "first_name": data["first_name"],
        "last_name": data["last_name"],
        "address1": data["address"],
        "city": data["city"],
        "province": data["state"],
        "zip": data["zip"],
        "country": "United States",
        }
    ]
    }

    if list_id:
        subs["subscribers"][0]["list_id"] = list_id
    


    # Send the POST request
    response = requests.post(url, headers=headers, json=subs)

    return response.status_code

# bayengage_integration('988925cac9ce56992deb15eb71daad63','727f10444e1a', data)


def klayviyo_integration(api_key, data):
    url = "https://a.klaviyo.com/api/profiles/"

    headers = {
        "accept": "application/json",
        "revision": "2023-10-15",
        "content-type": "application/json",
        "Authorization": f"Klaviyo-API-Key {api_key}"
    }

    payload = {"data": {
        "type": "profile",
        "attributes": {
            "email": data["email"],
            "external_id": data["full_name"],
            "first_name": data["first_name"],
            "last_name":  data["last_name"],
            "location": {
                "address1": data["address"],
                "city": data["city"],
                "country": "United States",
                "region": data["state"],
                "zip": str(data["zip"]),
                "ip": data["opt_in_ip"]
            },
            "properties": data
        }
    }}

    response = requests.post(url, json=payload, headers=headers)

    print(response.text)
    return int(response.status_code)

# klayviyo_integration('pk_765b0c425e3d77fcbb16c0bc6d6c970c4f',data)


def customerIO_integration(site_id, api_key,data):
    try:
        cio = CustomerIO(site_id, api_key, region=Regions.US)
        cio.identify(id=data['full_name'], plan='premium', **data)

        return 201
    except Exception as e:
        print(e)
        return 500

# customerIO_integration('e86d95116ef5e5381a2a', 'a658e094ff18f06d86b4', data)


def mailchimp_integration(api_key, list_id, data):
    try:
        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": api_key,
            "server": api_key.split("-")[-1]
        })

        client.lists.add_list_member(
            list_id,
            {
                "skip_merge_validation": True,
                "email_address": data["email"],
                "status": "unsubscribed",
                "ip_opt": data["opt_in_ip"],
                "merge_fields": {
                    "FNAME": data['first_name'],
                    "LNAME": data["last_name"],
                    "EMAIL": data["email"],
                    "PHONE": format_phone_number(data["phone"]),
                    "ADDRESS": {
                        "addr1": data["address"],
                        "city": data['city'],
                        "state": data["state"],
                        "zip": str(data["zip"])
                    },
                }
            }
        )
        return 200

    except ApiClientError as error:
        print("Error: {}".format(error.text))
        return 500

# mailchimp_integration("3bf4a01ec35ec863f203bd238e59c5ff-us21","4f8553254d",data)


def sendgrid_integration(api_key, data, lists):
    url = "https://api.sendgrid.com/v3/marketing/contacts"

    # Set the headers
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Set the request body
    data = {
        "list_ids": lists,
        "contacts": [
            {
                "address_line_1": data["address"],
                "city":  data["city"],
                "country": "United States",
                "email": data["email"],
                "first_name": data["first_name"],
                "last_name": data["last_name"],
                "postal_code": str(data["zip"]),
                "phone_number": format_phone_number(data["phone"])
            }
        ]
    }

    # Send the PUT request
    response = requests.put(url, headers=headers, json=data)
    print(response.text)
    # Check the response
    return response.status_code 


# sendgrid_integration('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',data, ['a051ba2c-ae43-43a7-8fba-cfe5916a77bf'])


def hubspot_integration(api_key, data):
    url = 'https://api.hubapi.com/crm/v3/objects/contacts'

    headers = {
        "Content-Type": "application/json",
        "authorization": f'Bearer {api_key}'
    }

    data = {
        "properties": {
            "email": data["email"],
            "firstname": data["first_name"],
            "lastname": data["last_name"],
            "phone": format_phone_number(data["phone"]),
            "company": "IceWeb IO",
            "website": "iceweb.io",
            "lifecyclestage": "marketingqualifiedlead"
        }
    }

    # Send the POST request
    response = requests.post(url, headers=headers, json=data)

    return response.status_code


# hubspot_integration('pat-na1-cec38f84-24b3-4a96-a2c2-de093a7b8faa',data)


def omnisend_integration(api_key, data):
    url = "https://api.omnisend.com/v3/contacts"

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "X-API-KEY": api_key
    }


    payload = {
        "firstName": data["first_name"],
        "lastName": data["last_name"],
        "country": "United States",
        "state": data["state"],
        "city": data["city"],
        "address": data["address"],
        "postalCode": str(data["zip"]),
        "birthdate": get_birthdate_from_age(data["age"]),
        "customProperties": data,
        "identifiers": [
            {
                "type": "email",
                "consent": {
                "source": data['opt_in_url'],
                "ip": data["opt_in_ip"]
            },
                "channels": {
                    "email": {"status": "subscribed"}
                },
                "id": data["email"]
            }
        ]
    }

    if data["gender"] != "-":
        payload["gender"] = data["gender"][0].lower()

    response = requests.post(url, json=payload, headers=headers)

    print(response.text)
    return response.status_code

# omnisend_integration("656df335237faa9cf404cf33-dRP8r7WyHe61JhTOFJ07s9j1g67m5DutDqqoPsPHWoWsp8jEp4",data)


def emailoctopus_integration(api_key, list_id, data,test=None):
    contact_url = f'https://emailoctopus.com/api/1.6/lists/{list_id}/contacts'
    field_url = f'https://emailoctopus.com/api/1.6/lists/{list_id}/fields'

    headers = {
        'Content-Type': 'application/json',
    }

    data_keys_list = list(data.keys())

    if test:
        for key in data_keys_list:
            if any([key == 'address', key=='city',key== 'state',key == 'gender',key=='maid', key=='age']):
                field = {
                    "api_key": api_key,
                    "label": key,
                    "tag": key,
                    "type": "TEXT",
                    "fallback": "Unknown"
                }
                response = requests.post(field_url, headers=headers, json=field)
                print(response.text)
            elif key == 'maid_os':
                field = {
                    "api_key": api_key,
                    "label": 'maidOS',
                    "tag": 'maidOS',
                    "type": "TEXT",
                    "fallback": "Unknown"
                }
                response = requests.post(field_url, headers=headers, json=field)
                print(response.text)
            

    payload = {
        "api_key": api_key,
        "email_address": data['email'],
        "fields": {
            "FirstName": data["first_name"],
            "LastName": data["last_name"],
            "address": data["address"],
            "city": data["city"],
            "state": data["state"],
            "gender": data["gender"],
            "maid": data["maid"],
            "maidOs": data["maid_os"],
        },
        "tags": [
            "vip"
        ],
        "status": "SUBSCRIBED"
    }

    if data["age"] != '-':
        payload["age"] = int(float(data["age"]))
        

    response = requests.post(contact_url, headers=headers, json=payload)

    print(response.text)
    return response.status_code

# emailoctopus_integration('9d824b7b-3f8e-470f-b069-3bcaa586a733','485ed3f4-92bd-11ee-9568-ed37a2e1a062',data)


# url = "https://YOUR_SUBDOMAIN.rest.marketingcloudapis.com/contacts/v1/contacts"
# headers = {
#     "Content-Type": "application/json",
#     "Authorization": "Bearer YOUR_ACCESS_TOKEN"
# }

# data = {
#     "contactKey": "acruz@example.com",
#     "attributeSets": [
#         {
#             "name": "Email Addresses",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Email Address", "value": "acruz@example.com"},
#                         {"name": "HTML Enabled", "value": True}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "Email Demographics",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Last Name", "value": "Cruz"},
#                         {"name": "First Name", "value": "Angela"},
#                         {"name": "Text Profile Attribute", "value": "value 1"},
#                         {"name": "Number Profile Attribute", "value": 12345}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "MobileConnect Demographics",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Mobile Number", "value": "317-531-5555"},
#                         {"name": "Locale", "value": "US"},
#                         {"name": "Status", "value": 1}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "MobilePush Demographics",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Device ID", "value": 958405948},
#                         {"name": "Application", "value": 958405948}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "GroupConnect LINE Addresses",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Address ID", "value": "addressId_from_api"}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "GroupConnect LINE Subscriptions",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Address ID", "value": "addressId_from_api"},
#                         {"name": "Channel ID", "value": "1234567890"}
#                     ]
#                 }
#             ]
#         },
#         {
#             "name": "GroupConnect LINE Demographics",
#             "items": [
#                 {
#                     "values": [
#                         {"name": "Address ID", "value": "addressId_from_api"},
#                         {"name": "Display Name", "value": "display_name"},
#                         {"name": "Picture Url", "value": "picture_url"},
#                         {"name": "Status Message", "value": "status_message"}
#                     ]
#                 }
#             ]
#         }
#     ]
# }

# response = requests.post(url, json=data, headers=headers)

# print("Response Status Code:", response.status_code)
# print("Response Content:", response.text)
