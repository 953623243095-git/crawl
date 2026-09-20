"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 395 / 400
================================================================================
- Group: ThirdTest_pincode_group_40_parts_391_to_400
- Assigned PIN Codes: 48 (Range: 845420 to 847109)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_395.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_395.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_395"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-395] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "845420",
  "845422",
  "845423",
  "845424",
  "845425",
  "845426",
  "845427",
  "845428",
  "845429",
  "845430",
  "845431",
  "845432",
  "845433",
  "845434",
  "845435",
  "845436",
  "845437",
  "845438",
  "845440",
  "845449",
  "845450",
  "845451",
  "845452",
  "845453",
  "845454",
  "845455",
  "845456",
  "845457",
  "845458",
  "845459",
  "846001",
  "846002",
  "846003",
  "846004",
  "846005",
  "846006",
  "846007",
  "846008",
  "846009",
  "847101",
  "847102",
  "847103",
  "847104",
  "847105",
  "847106",
  "847107",
  "847108",
  "847109"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "845420": {
    "pincode": "845420",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Gulwara Madhuban SO",
      "Bazitpur BO",
      "Bhelwa BO",
      "Kauriya BO",
      "Khodadpur BO",
      "Koilhara BO",
      "Krishna Nagara BO",
      "Madhurapur BO",
      "Moglania BO",
      "Naurangia BO",
      "Rupani BO"
    ]
  },
  "845422": {
    "pincode": "845422",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Harsiddhi SO",
      "Balua BO",
      "Dhawahi BO",
      "Jadopur BO",
      "Kanchhedwa BO",
      "Mankararia BO",
      "Math Lohiyar BO",
      "Nonea BO",
      "Paharia BO",
      "Pannapur Ranjeeta BO",
      "Panditpur BO",
      "Rai Kararia BO",
      "Siswa Bazar BO",
      "Son Barsa BO"
    ]
  },
  "845423": {
    "pincode": "845423",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Hussaini SO",
      "Belwa Madho BO",
      "Dumariaghat BO",
      "Karhan BO",
      "Mangalpur BO",
      "Purvi Pakari BO",
      "Rampur Khajuria BO",
      "Sarotar BO",
      "Sembhuapur BO"
    ]
  },
  "845424": {
    "pincode": "845424",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Kesharia SO",
      "Bairiya BO",
      "Bankat BO",
      "Bijdhari Nizamat BO",
      "Kunwar Dhekha BO",
      "Naya Gaon BO",
      "Sundarpur BO",
      "Tajpur Patkhaulia BO"
    ]
  },
  "845425": {
    "pincode": "845425",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Malahi SO",
      "Bharwalia BO",
      "Chatia BO",
      "Damodarpur Mathia BO",
      "MSikatia BO",
      "Mamarkha BO",
      "Nagdaha BO",
      "Sirni Bazar BO",
      "Sonwal BO"
    ]
  },
  "845426": {
    "pincode": "845426",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Mehsi SO",
      "Bans Ghat BO",
      "Dilawarpur BO",
      "Gawandra BO",
      "Garahiya BO",
      "KHariram BO",
      "Kasba Mehsi BO",
      "Kataha BO",
      "Madan Sirsiya BO",
      "Madhopur Govind BO",
      "Mahamada BO",
      "Puran Chapra BO",
      "Sirsia Ganesh BO"
    ]
  },
  "845427": {
    "pincode": "845427",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Pachpakari SO",
      "Bhandar BO",
      "Dalpat Bishunpur BO",
      "Gonahi BO",
      "Jhitkahiya BO",
      "Jihuli BO",
      "Khori Pakar BO",
      "Narayanpur BO",
      "Noonfarwa BO",
      "Padmuker BO"
    ]
  },
  "845428": {
    "pincode": "845428",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Pakari Dayal SO",
      "Ajgarwa BO",
      "Barkagaon BO",
      "Dhanujee BO",
      "Itwa BO",
      "Majhar Gohiya BO",
      "Sishani BO"
    ]
  },
  "845429": {
    "pincode": "845429",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Pipra Factory SO",
      "Ashok Pakari BO",
      "Bangari BO",
      "Belwatia BO",
      "Chandrahiya BO",
      "Jhakhara BO",
      "Jiwdhara BO",
      "MBBathana BO",
      "Mathurapur BO"
    ]
  },
  "845430": {
    "pincode": "845430",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Phenhara SO",
      "Bisunpur Basant BO",
      "Deokulia BO",
      "Ibrahimpur Parsauni BO",
      "Izor Bara BO",
      "Kalu Pakar BO",
      "Kodariya BO",
      "Kumhrar BO",
      "Mankarwa BO",
      "Muzia BO"
    ]
  },
  "845431": {
    "pincode": "845431",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Raghunathpur Bazar SO",
      "Gamharia BO",
      "Jhitkahia BO",
      "Kukurjari BO",
      "Mansingha BO",
      "Pajiarwa BO",
      "Phulwar BO",
      "Madhopur Karamwa BO"
    ]
  },
  "845432": {
    "pincode": "845432",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Rajpur SO East Champaran",
      "Bhgwatia BO",
      "Bhopatpur BO",
      "Darmaha BO",
      "Gariba BO",
      "Hazipur BO",
      "Jasauli Jamunia BO",
      "Konhia BO",
      "Raghunathpur BO",
      "Siswa Kharar BO",
      "Siswa Patna BO"
    ]
  },
  "845433": {
    "pincode": "845433",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Ramgahrwa SO",
      "Ahiraulia BO",
      "Amodei BO",
      "Andhra BO",
      "Bagahi BO",
      "Bairiya BO",
      "Bela BO",
      "Chainpur BO",
      "Champapur BO",
      "Dhanhar Dihuli BO",
      "Inrawa BO",
      "Laxmipur Pokhariya BO",
      "Majharia BO",
      "Murla BO",
      "Parsauna Madan BO",
      "Patani BO",
      "Raghunathpur BO",
      "Singasani BO",
      "Tola Mauje BO",
      "Adhkaparia Phulwaria BO"
    ]
  },
  "845434": {
    "pincode": "845434",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Sangrampur SO East Champaran",
      "Bariariya BO",
      "Manglapur Rajpur BO",
      "Puchharia BO",
      "Sikandarpur BO",
      "Thikaha Bhawanipur BO"
    ]
  },
  "845435": {
    "pincode": "845435",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Semra SO East Champaran",
      "BRTNariarwa BO",
      "Bairiadih BO",
      "Belghati BO",
      "Belwa Rai Khas BO",
      "Bhargawan BO",
      "Chhapra Bahas BO",
      "Janerwa  BO",
      "Khagani BO",
      "Mukhalishpur Pachrukha BO",
      "Sapaha BO"
    ]
  },
  "845436": {
    "pincode": "845436",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Tetaria Factory SO",
      "Bahuarwa Gopi Singh BO",
      "Gheghawa BO",
      "Kajraha BO",
      "Madhuwahan Brit BO",
      "Semraha BO"
    ]
  },
  "845437": {
    "pincode": "845437",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Turkaulia SO",
      "Sabaiya BO",
      "Baharupia BO",
      "Barharwa Kala BO",
      "Barmashwa BO",
      "Chainpur BO",
      "Gai Ghat BO",
      "Jagiraha BO",
      "Jaishingpur BO",
      "Jasauli Patti BO",
      "Kotwa BO",
      "Kushar BO",
      "Madhopur BO",
      "Makhuwa BO",
      "Manikpur BO",
      "Murarpur BO",
      "Sapahi BO",
      "Shankar Sharaiya BO",
      "Talwa Pokhar BO",
      "Tikaita BO"
    ]
  },
  "845438": {
    "pincode": "845438",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Bettiah HO",
      "Barwat Pasrain BO",
      "Barwat Sena BO",
      "Dhum Nagar BO",
      "Banu Chhapar BO",
      "Bettiah Dih BO",
      "Gonauli BO",
      "Lalgarh BO",
      "Mahna Gani BO",
      "Pipra Naurangia BO",
      "Pipra Pakari BO",
      "Alpaha BO",
      "B Patkhaulia BO",
      "B Ratanpura BO",
      "Baijua BO",
      "Bairiya Farm BO",
      "Balua Rampurwa BO",
      "Bhawanipur BO",
      "Bhitaha Nizamat BO",
      "Gambhirpur BO",
      "Khadda BO",
      "Laukariya BO",
      "Nautan Dubey BO",
      "Pakhanaha Bazar BO",
      "Patzirwa BO",
      "Pokharia BO",
      "Refujee Camp BO",
      "San Sariya BO",
      "Sareya Ojhwalia BO",
      "Sheorajpur BO",
      "Surajpur BO",
      "Tadhawa Nandpur BO",
      "Belbagh SO",
      "Bettiah RS SO",
      "Lal Bazar Bettiah SO",
      "Meena Bazar Bettiah SO",
      "Naya Tola Bettiah SO",
      "KRHigh School BO",
      "B K Chowk BO"
    ]
  },
  "845440": {
    "pincode": "845440",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Jaitapur Laukaria SO",
      "Gad Bahuari BO",
      "Laukaria BO",
      "ParsaunaTapasi BO",
      "Palanwa BO",
      "Sakarar BO"
    ]
  },
  "845449": {
    "pincode": "845449",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Chanpatia R S SO",
      "Bakulahar Math BO",
      "Belwa BO",
      "Bhabhata BO",
      "Bheriharwa BO",
      "Chaubey Tola BO",
      "Danial Parsauna BO",
      "Dhamaura BO",
      "Dhankutwa BO",
      "Dharampur BO",
      "Ghogha BO",
      "Jaitia BO",
      "Pokharia Rai BO",
      "Puraina Gosai BO",
      "Sathi BO",
      "Sihpur BO",
      "Tikulia BO",
      "Chanpatia Bazar SO"
    ]
  },
  "845450": {
    "pincode": "845450",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Chuhari SO",
      "Ashram Brindavan BO",
      "Auraiya BO",
      "Gurwalia BO",
      "Jado Chhapar BO",
      "Kurwa Mathia BO",
      "Laxmipur BO",
      "Mushahari BO",
      "Sirisia BO",
      "Turahapatti BO"
    ]
  },
  "845451": {
    "pincode": "845451",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "DKShikarpur SO",
      "Bargajwa BO",
      "Chauhatta BO",
      "Damrapur BO",
      "Gokhula BO",
      "Maldi BO",
      "Mangrahari BO",
      "Maniari BO",
      "Padraun BO",
      "Rakhahi BO",
      "Rampur BO"
    ]
  },
  "845452": {
    "pincode": "845452",
    "circle": "Bihar circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Bagahi BO",
      "Barwa Ojha BO",
      "Chamukha BO",
      "Dudhiawa BO",
      "Dumari Bazar BO",
      "Jogapatti SO",
      "Jaralpur Dih BO",
      "Machhargawan BO",
      "Nawalpur BO",
      "Puraina BO",
      "Shiorajpur BO",
      "Sirnagar BO"
    ]
  },
  "845453": {
    "pincode": "845453",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Lauriya SO",
      "Bariarwa BO",
      "Baswariya BO",
      "Belwa More BO",
      "Bijbania BO",
      "Briti Matiaria BO",
      "Dhobini BO",
      "Dumara BO",
      "Gobraura BO",
      "Kehunia BO",
      "Lakhanpur BO",
      "Mathia BO",
      "Parsa Factory BO",
      "Parsauni Farm BO",
      "Ramparsauna BO",
      "Roari BO",
      "Sahadat Pur BO",
      "Siswa Basantpur BO",
      "Siswania BO"
    ]
  },
  "845454": {
    "pincode": "845454",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Majhaulia Rs SO",
      "Bakharia BO",
      "Chailabhar BO",
      "Chanayan Bandh BO",
      "Dhokharaha BO",
      "Gora Karamwa BO",
      "Harpur Tola BO",
      "Jawkatia BO",
      "Kathaia BO",
      "Lal Saraiya BO",
      "Madhopur BO",
      "Majhariya Sheikh BO",
      "Mathia Brit BO",
      "Mohachhinain BO",
      "Rajabhar BO",
      "Rampurwa BO",
      "Rampurwa Mahanwa BO",
      "Ratanmala Mathia BO",
      "Rulahi Nizamat BO",
      "Sariswa Bazar BO",
      "Senwaria BO",
      "Shikarpur BO"
    ]
  },
  "845455": {
    "pincode": "845455",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Narkaiaganj SO",
      "Amolwa BO",
      "Anjua BO",
      "Belsandi BO",
      "Belwa Bahuari BO",
      "Bhikhana Thori BO",
      "Bhitiharwa Ashram BO",
      "Goithahi BO",
      "Jamunia BO",
      "Lachhnauta BO",
      "Mahuawa BO",
      "Manwa Parsi BO",
      "Musharwa BO",
      "Rajpur Madan BO",
      "Saidpur BO",
      "Serwa Masjidwa BO",
      "Sugauli BO",
      "Taulaha Champaran BO"
    ]
  },
  "845456": {
    "pincode": "845456",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Sugauli SO",
      "Bahuarawa BO",
      "Bhataha BO",
      "Chhapawa Phulwaria BO",
      "Gudra BO",
      "Kaithwalia BO",
      "Mali BO",
      "Parsa BO",
      "Sripur BO",
      "Sugaon BO",
      "Sugauli Bazar BO",
      "Sukul Pakar BO"
    ]
  },
  "845457": {
    "pincode": "845457",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Patahi SO East Champaran",
      "Bakhari BO",
      "Balua Zulfikarabad BO",
      "Bara Shankar BO",
      "Bela Baiju BO",
      "Betauna BO",
      "Mirzapur BO",
      "Parsauni Kapoor BO",
      "Saraiya Gopal BO"
    ]
  },
  "845458": {
    "pincode": "845458",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "East Champaran Division",
    "offices": [
      "Manguraha SO",
      "Chhapkahia BO",
      "Ganga Pipara BO",
      "Nawadih BO",
      "Paharpur BO",
      "Radhia BO",
      "Rampurwa Bazar BO"
    ]
  },
  "845459": {
    "pincode": "845459",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "West Champaran Division",
    "offices": [
      "Jagdishpur SO West Champaran",
      "Ahwar Shaikh BO",
      "Alamganj Bazar BO",
      "Amwa Majhar BO",
      "B Banahura BO",
      "Bishabhar Pur BO",
      "Bisunpura BO",
      "Jagarnath Pur BO",
      "Jhakhara BO",
      "Majharia Kisun BO",
      "Mangalpur Gudaria BO",
      "Ramnagar Bankat BO",
      "Sareya BO",
      "Shyampur Kotaraha BO",
      "Teluha BO",
      "Victoria Mission BO"
    ]
  },
  "846001": {
    "pincode": "846001",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Laheriasarai HO",
      "Imambari SO",
      "Laheriasarai Court SO"
    ]
  },
  "846002": {
    "pincode": "846002",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "SHMC SO",
      "BMVPith BO",
      "Bahadurpur BO",
      "Chandanpatti BO",
      "Dekuli BO",
      "MPBahadurpur BO",
      "RAPMills BO",
      "Ratanpura BO",
      "Sirnia BO",
      "Thalwara BO"
    ]
  },
  "846003": {
    "pincode": "846003",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "DMC SO",
      "Dihrampur BO",
      "Dilahi BO",
      "Goriari BO",
      "Kharajpur BO",
      "MSinuara BO",
      "Narsara BO",
      "Ojhaul BO",
      "PBasant BO",
      "Panchobe BO",
      "Rupauli Ghat BO",
      "Taralahi BO",
      "Benta South SO"
    ]
  },
  "846004": {
    "pincode": "846004",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Darbhanga HO",
      "C M College SO",
      "Darbhanga Bara Bazar SO",
      "Darbhanga Chowk SO",
      "Darbhanga City SO",
      "Kathalwari SO",
      "Kilaghat SO",
      "Katki Bazar BO",
      "Qadirabad BO"
    ]
  },
  "846005": {
    "pincode": "846005",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Postal Traning Centre SO",
      "BBira BO",
      "BKAsthan BO",
      "Barhi BO",
      "Bariaul BO",
      "Lalsahpur BO",
      "Siso BO",
      "Sisodih BO",
      "Sonhaun BO"
    ]
  },
  "846006": {
    "pincode": "846006",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Subhankarpur SO"
    ]
  },
  "846007": {
    "pincode": "846007",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Saramohanpur SO",
      "Atihar BO",
      "Bhuskaul BO",
      "GBasailha BO"
    ]
  },
  "846008": {
    "pincode": "846008",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "K D S U SO"
    ]
  },
  "846009": {
    "pincode": "846009",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Laxmisagar SO Darbhanga",
      "Dhoi BO",
      "Kabaria Khutwara BO",
      "Kabirchak BO",
      "Kapchhahi BO",
      "Mahpara BO",
      "Milkichak BO",
      "Sonki BO"
    ]
  },
  "847101": {
    "pincode": "847101",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Anandpur SO",
      "Baligaon BO",
      "Bhacchi BO",
      "Chilha Dilawarpur BO",
      "Dhanauli BO",
      "Gujrauli BO",
      "Harhachcha BO",
      "Hawasa BO",
      "Jogiara BO",
      "Jorja BO",
      "Khaira Kunji BO",
      "Madanpur BO",
      "Mirzapur BO",
      "Nimathi BO",
      "Patore BO",
      "Shivram BO",
      "Sidhauli BO",
      "Sirua Maner BO",
      "Srirampur BO",
      "Ughra BO"
    ]
  },
  "847102": {
    "pincode": "847102",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Madhubani Division",
    "offices": [
      "Basaith Chandpura SO",
      "Barri BO",
      "Bisanpur BO",
      "DChandpura BO",
      "Karbi Dhankaul BO",
      "Massa BO",
      "Meghwan BO",
      "Shahpur BO",
      "Shivnagar BO",
      "Taraiya BO"
    ]
  },
  "847103": {
    "pincode": "847103",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Benipur SO",
      "Andauli BO",
      "Antaur BO",
      "Bhawanipur BO",
      "Harsinghpur BO",
      "Jayantipur BO",
      "KJagdishpur BO",
      "Korthu BO",
      "Mahinam BO",
      "Mohiuddinagar Pakari BO",
      "Motipur BO",
      "Nari BO",
      "Neori BO",
      "Pohhadi BO",
      "Rohar BO",
      "Ruchighat BO",
      "Shivnagarghat BO"
    ]
  },
  "847104": {
    "pincode": "847104",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Bharwara SO",
      "Ashthua BO",
      "Bhawanipur BO",
      "Kaligaon BO",
      "Kora BO",
      "Manikauli BO"
    ]
  },
  "847105": {
    "pincode": "847105",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Baheri SO Darbhanga",
      "Arga BO",
      "Baghauni BO",
      "Baghra Simri BO",
      "Bakmandal BO",
      "Boraj BO",
      "Inai BO",
      "Khangraitha BO",
      "Kothra BO",
      "Matunia BO",
      "Narayandohat BO",
      "PGhiwahi BO",
      "Paghari BO",
      "Ranna BO",
      "SMajarahia BO",
      "Thathopur BO"
    ]
  },
  "847106": {
    "pincode": "847106",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Kansi Simri SO",
      "Arai Birdipur BO",
      "Araila BO",
      "Barhaulia BO",
      "Moro BO",
      "Ratanpura BO",
      "Sarwara BO"
    ]
  },
  "847107": {
    "pincode": "847107",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Darbhanga Division",
    "offices": [
      "Keotsa Baruari SO",
      "Balour BO",
      "Baruary BO",
      "Benibad BO",
      "Gaighat BO",
      "Janta BO",
      "KPirochha BO",
      "Ladour BO",
      "Laxman Nagar BO",
      "Maheshwara BO",
      "Seodaha Barail BO",
      "Subhash Keso BO",
      "Susta BO",
      "Tharma BO"
    ]
  },
  "847108": {
    "pincode": "847108",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Madhubani Division",
    "offices": [
      "Narahiya SO",
      "Laukahi Dhanchhiha BO",
      "Dhanchhiha Baruar BO",
      "Auraha BO",
      "Bangama BO",
      "Belhi Bhawanipur BO",
      "Bhaptiahi BO",
      "Bharkhori BO",
      "Chhajana BO",
      "Chichorba BO",
      "Dhabahi BO",
      "Kakardobh BO",
      "Karhari BO",
      "Kariot BO",
      "Kukurdaura BO",
      "Laukahi BO",
      "Makhnaha BO",
      "Mansapur BO",
      "Neor BO",
      "Rajaram Patti BO",
      "Ratnasara BO"
    ]
  },
  "847109": {
    "pincode": "847109",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Madhubani Division",
    "offices": [
      "Tulapatganj SO",
      "Araria Sangra BO",
      "Mahindwar Navtoli BO",
      "Nanaour BO",
      "Parsa BO",
      "Sirkharia BO",
      "Tazpur BO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
