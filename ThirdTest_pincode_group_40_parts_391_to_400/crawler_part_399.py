"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 399 / 400
================================================================================
- Group: ThirdTest_pincode_group_40_parts_391_to_400
- Assigned PIN Codes: 48 (Range: 852114 to 854113)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_399.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_399.csv & .json
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

PART_ID = "part_399"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-399] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "852114",
  "852115",
  "852116",
  "852121",
  "852122",
  "852123",
  "852124",
  "852125",
  "852126",
  "852127",
  "852128",
  "852129",
  "852130",
  "852131",
  "852132",
  "852133",
  "852137",
  "852138",
  "852139",
  "852161",
  "852201",
  "852202",
  "852212",
  "852213",
  "852214",
  "852215",
  "852216",
  "852217",
  "852218",
  "852219",
  "852220",
  "852221",
  "853201",
  "853202",
  "853203",
  "853204",
  "853205",
  "854101",
  "854102",
  "854103",
  "854104",
  "854105",
  "854106",
  "854107",
  "854108",
  "854109",
  "854112",
  "854113"
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
  "852114": {
    "pincode": "852114",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Budhma SO",
      "Belari BO",
      "Belo BO",
      "Bhadol BO",
      "Bhairo Patti BO",
      "Bhatkhora BO",
      "Dhurgaon BO",
      "Jeeta Pur BO",
      "Murho BO",
      "Narsingbag BO",
      "PNPur BO",
      "Pokhram BO",
      "Rampatti BO",
      "SMillick BO"
    ]
  },
  "852115": {
    "pincode": "852115",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Gwalpara SO",
      "Anandpura BO",
      "Basnahi BO",
      "Bishanpurarar BO",
      "Chatra BO",
      "Jhalari BO",
      "Jhanjhari BO",
      "Mohamadpur BO",
      "Peernagar BO",
      "Shahpur BO",
      "Sukhashan BO",
      "Temabhelwa BO"
    ]
  },
  "852116": {
    "pincode": "852116",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Puraini Bazar SO",
      "Akbarpur BO",
      "Aurai BO",
      "Durgapur BO",
      "Kherho BO",
      "Manjoura BO",
      "Naya Tola BO",
      "Rahua BO"
    ]
  },
  "852121": {
    "pincode": "852121",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Mathahi SO",
      "Arraha BO",
      "BGarhia BO",
      "BTekhti BO",
      "Basudeva BO",
      "SDakshinwari BO",
      "SSimraha BO",
      "T Dhanchhoa BO"
    ]
  },
  "852122": {
    "pincode": "852122",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Murliganj SO",
      "Amarpura BO",
      "Bhangha Chandpur BO",
      "Dinapatti BO",
      "Gangapur BO",
      "HKala BO",
      "Hareshwari Dighi BO",
      "Jorgama BO",
      "Lc Ashthan BO",
      "Mirchaibari BO",
      "Murliganj Bazar BO",
      "Murliganj Tola BO",
      "Naulakhi BO",
      "Parwa BO",
      "Raghunath Pur BO",
      "Rahta BO",
      "Rampur BO",
      "Ratanpatti BO",
      "Sahuria BO",
      "Tamot Persa BO"
    ]
  },
  "852123": {
    "pincode": "852123",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Nauhatta SO",
      "Baira BO",
      "Bakunia BO",
      "Barharia BO",
      "Chandryan Dharhara BO",
      "Darhar BO",
      "Dhrampur BO",
      "Ekrah BO",
      "Kataiya BO",
      "Kedlipatti BO",
      "Mohanpur BO",
      "Muradpur BO",
      "Ramnagar Bharna BO",
      "Rasalpur BO",
      "Shahpur Manjhol BO"
    ]
  },
  "852124": {
    "pincode": "852124",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Panchgachiya SO",
      "Aran BO",
      "Bara BO",
      "Barhsher BO",
      "Bhatrandha BO",
      "Bijalpur BO",
      "Chikni Phulkaha BO",
      "Chitti BO",
      "GBihra BO",
      "Ghailar BO",
      "Jiwachpur BO",
      "Manjhaul BO",
      "Menha Khadimpur BO",
      "Nandalali BO",
      "Patori BO",
      "Purshotampur BO",
      "RTulsiahi BO",
      "Sattar BO",
      "Sihaul BO",
      "Srinagar BO"
    ]
  },
  "852125": {
    "pincode": "852125",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Pratapganj SO Supaul",
      "Bhawanipur Patni BO",
      "Chilauni BO",
      "Gonaha BO",
      "Govindpur BO",
      "Jiwachhpur BO",
      "Madhubani BO",
      "Sukhanagar BO",
      "Surjapur BO",
      "Tekuna BO"
    ]
  },
  "852126": {
    "pincode": "852126",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Salkhua Bazar SO",
      "Alani BO",
      "Bhelwa BO",
      "Harewa BO",
      "Itahari BO",
      "Jamal Nagar BO",
      "Koparia BO",
      "Mobarakpur BO",
      "Uteshra BO"
    ]
  },
  "852127": {
    "pincode": "852127",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Simri Bakhtiarpur SO",
      "Balhampur BO",
      "Bariarpur BO",
      "Bhatauni BO",
      "Ghordaur BO",
      "Gordah BO",
      "Goriari BO",
      "Khamauti BO",
      "Mahkhar BO",
      "Paharpur BO",
      "Rahua BO",
      "Sarbella BO",
      "Sardiha BO",
      "Simri BO",
      "Sonpura BO",
      "Tariama BO"
    ]
  },
  "852128": {
    "pincode": "852128",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Singheshwar SO",
      "Kataia Satokar BO",
      "Lalpur Sarropatti BO",
      "Maheshua BO",
      "Majarhat BO",
      "Manhara Sukhashan BO",
      "Manpur BO",
      "Patori BO",
      "Piprahi BO",
      "Raibhir BO",
      "Ratanpura BO",
      "Rupauli Jiwachpur BO",
      "Rupauli Lalpatti BO",
      "Sigion BO",
      "Sripur Tengraha BO",
      "Barahi Hasanpur BO",
      "Behrari BO",
      "Behri BO",
      "Bhawanipur BO",
      "Gahumani BO",
      "Gidha BO",
      "Hanuman Nagar Chouraha BO"
    ]
  },
  "852129": {
    "pincode": "852129",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Sonbersa Raj SO",
      "Dehad BO",
      "Kenjari BO",
      "Lagma Deorhi BO",
      "Maina BO",
      "Mali BO",
      "Pararia BO",
      "Rasalpur BO",
      "Sahsoul BO",
      "Shahpur BO",
      "Soha BO",
      "Sugma BO"
    ]
  },
  "852130": {
    "pincode": "852130",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Sukhpur SO",
      "Bakaur BO",
      "Balha BO",
      "Barahihat BO",
      "Basbitti BO",
      "Dumra BO",
      "Hati BO",
      "Nemua BO",
      "Parsharma BO",
      "Telwa Situhar BO"
    ]
  },
  "852131": {
    "pincode": "852131",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Supaul HO",
      "Babhani Bhelwa BO",
      "Basaha BO",
      "Bela Terha BO",
      "Bina BO",
      "Chainsinghpatti BO",
      "Dabhari BO",
      "Dighiya BO",
      "Ghuran BO",
      "Gidarahi BO",
      "Jobaha BO",
      "Jolahaniya BO",
      "Kadampura BO",
      "Kariho BO",
      "Karupur BO",
      "Kataia Mahe BO",
      "Kharail BO",
      "Khokhnaha BO",
      "Laurh BO",
      "Maujaha BO",
      "Monga Sihol BO",
      "Nirmali BO",
      "Pathra BO",
      "Piprakhurd BO",
      "Thumha BO",
      "Supaul Bazar SO"
    ]
  },
  "852132": {
    "pincode": "852132",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Shankarpur Bazar SO",
      "Jirwa BO",
      "Jirwa Madheli BO",
      "Kalhua BO",
      "Maoura Ramnagar BO",
      "Shankarpur BO",
      "Sonbersa BO",
      "Chouraha BO",
      "Bariahi BO",
      "Bathanparsa BO",
      "Jharkaha BO"
    ]
  },
  "852133": {
    "pincode": "852133",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Baskhora BO",
      "Janardanpur SO",
      "Marauna BO",
      "Ganaura BO",
      "Ganeshpur BO",
      "Kamrail BO",
      "Khorma BO"
    ]
  },
  "852137": {
    "pincode": "852137",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Surpatganj SO",
      "Chunnimal BO",
      "Daharia BO",
      "Ghiwaha BO",
      "Girdharpatti BO",
      "Gwalpara BO",
      "Hariharpur BO",
      "Khunti BO",
      "Lalganj BO",
      "Lalpur BO",
      "Mohanpur BO",
      "Mohmmad Ganj BO",
      "Pariyahi BO"
    ]
  },
  "852138": {
    "pincode": "852138",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Tharbitta SO",
      "Abhuar BO",
      "Bauraha BO",
      "Chauhatta BO",
      "Malarh BO",
      "Narahi BO",
      "Nauwa Bakhar BO",
      "Parsa Madho BO",
      "Ratauli BO",
      "Singihaun BO",
      "Sripur Sukhasan BO"
    ]
  },
  "852139": {
    "pincode": "852139",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Triveniganj SO",
      "Babhangama BO",
      "Baghla BO",
      "Baljora BO",
      "Barkurwa BO",
      "Bhura BO",
      "Daparkha BO",
      "Jaraila BO",
      "Karibiahi BO",
      "Kumiahi BO",
      "Kushaha BO",
      "Laharnia BO",
      "Latauna BO",
      "Malhanwa BO",
      "Mirjawa BO",
      "Orlaha Bazitpur BO"
    ]
  },
  "852161": {
    "pincode": "852161",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Begusarai Division",
    "offices": [
      "Beldaur SO",
      "Belanawada BO",
      "Chorhli BO",
      "Dhari Pachout BO",
      "Dighaun BO",
      "Dumri BO",
      "Kurwan BO",
      "Mohinath Nagar BO",
      "Pachaut BO",
      "Pansalwa BO",
      "Phulwaria Deorhi BO",
      "Pirnagra Deorhi BO",
      "Rohiyama BO",
      "Sakrohar BO",
      "Telihar BO"
    ]
  },
  "852201": {
    "pincode": "852201",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Saharsa HO",
      "Saharsa Bazar SO",
      "Saharsa DistBoard SO"
    ]
  },
  "852202": {
    "pincode": "852202",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Saharsa Collec SO",
      "Amarpur BO",
      "BSPatti BO",
      "Basauna BO",
      "Benghaha BO",
      "Bharouli Dhakjhari BO",
      "Dhamsena BO",
      "Diwari BO",
      "Garhia BO",
      "Goth Bardaha BO",
      "Haripur BO",
      "Kahra BO",
      "Mohanpur Dudhela BO",
      "Murli Basantpur BO",
      "Nariyarh BO",
      "Patuaha BO",
      "Rampur BO",
      "Rupanagar BO",
      "Saharsa Basti BO",
      "Samda BO",
      "Simraha BO",
      "Sisai BO",
      "Sulindabad BO"
    ]
  },
  "852212": {
    "pincode": "852212",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Bangaon SO Saharsa",
      "Bariahi BO",
      "Chainpur BO",
      "Parri BO",
      "Rahuamani BO"
    ]
  },
  "852213": {
    "pincode": "852213",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Chausa SO Madhepura",
      "Abhirampur BO",
      "BBarhona BO",
      "Bansgopal BO",
      "Chiraouri BO",
      "DKalan BO",
      "Dhaneshpur BO",
      "Ghosai BO",
      "Laua Lagam BO",
      "Makdampur BO",
      "Paina BO"
    ]
  },
  "852214": {
    "pincode": "852214",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Jadia SO",
      "BFulkaha BO",
      "Bishanpur BO",
      "Charne BO",
      "Govinpur BO",
      "Guria BO",
      "Koria Patti BO",
      "Mahulia BO",
      "Manganj BO",
      "Manganj Bazar BO",
      "PPrayag BO",
      "Persa Garhi BO",
      "Raghunathpur BO",
      "Rajeshwari BO",
      "Tamua BO"
    ]
  },
  "852215": {
    "pincode": "852215",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Karjain Bazar SO",
      "Baisa BO",
      "Baisi BO",
      "Bauraha BO",
      "Chitti Hanuman Nagar BO",
      "Daulatpur BO",
      "Dumari BO",
      "Goshpur BO",
      "Hariraha BO",
      "Janak Lal Basanpatti BO",
      "Korlahi BO",
      "Motipur BO",
      "Ratanpura BO",
      "Sripur BO",
      "Tarhi Bazar BO"
    ]
  },
  "852216": {
    "pincode": "852216",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Mahishi SO",
      "Aina BO",
      "Arapatti BO",
      "B Bhelahi BO",
      "Baghwa BO",
      "KBalua BO",
      "Kundah BO",
      "Maina Gram BO",
      "Manuar BO",
      "Narayanpur BO",
      "Sarauni BO",
      "Sisauna BO",
      "Telhar BO",
      "Telwa BO",
      "Terhi BO"
    ]
  },
  "852217": {
    "pincode": "852217",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Mangwar SO",
      "Amrita BO",
      "Atalkha BO",
      "Balia BO",
      "Baraith BO",
      "Bargaon BO",
      "Barsam BO",
      "Bhaddi Durgapur BO",
      "Biratpur BO",
      "Jalsima BO",
      "Jamhara BO",
      "Khajurah BO",
      "Kishanpur BO",
      "Mokma BO",
      "Noneti BO"
    ]
  },
  "852218": {
    "pincode": "852218",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Pipra Bazar SO",
      "Amha BO",
      "Bheropatti BO",
      "Dinapatti BO",
      "Gelhia Bishanpur BO",
      "Kamargama BO",
      "Laxmipur BO",
      "Pakri BO",
      "Persa BO",
      "Sakhua BO",
      "Simaria BO",
      "Tarhi Bhawanipur BO"
    ]
  },
  "852219": {
    "pincode": "852219",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Shah Alam Nagar SO",
      "Bajraha BO",
      "Basanwara BO",
      "Bispatti BO",
      "Gangapur BO",
      "Itahari BO",
      "KMillik BO",
      "KRatwara BO",
      "Khara Budhma BO",
      "Khurhan Mal BO",
      "Kunjouri BO",
      "Kurshandi BO",
      "Mahmooda BO",
      "Mahua Bazar BO",
      "Morsanda BO",
      "Muraut BO",
      "N Bhagipur BO",
      "Phulaut BO",
      "RNKhwan BO",
      "SMadheli BO",
      "Sahjadpur BO",
      "Sapardah BO",
      "Singhar BO",
      "TBargaon BO"
    ]
  },
  "852220": {
    "pincode": "852220",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Udakishunganj SO",
      "Gopalpur BO",
      "KSPur BO",
      "Karauti Bazar BO",
      "Laskari BO",
      "Murli Chandwa BO",
      "Naya Nagar BO",
      "RBarateni BO",
      "Rampur Khora BO",
      "Shyam BO"
    ]
  },
  "852221": {
    "pincode": "852221",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Sour Bazar SO",
      "Ajgeba BO",
      "Baijnathpur BO",
      "Barsam BO",
      "Chandour BO",
      "Damgarhi BO",
      "Gamaharia Rampur BO",
      "Garhia Vijaypur BO",
      "Kachra Karahia BO",
      "Kanp BO",
      "Khajuri BO",
      "Rauta BO",
      "Sahuria BO",
      "Suhath BO"
    ]
  },
  "853201": {
    "pincode": "853201",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Amarpur BO",
      "Babhangama BO",
      "Bharamarpur BO",
      "Dharampurrahi BO",
      "Gouripur BO",
      "Hario BO",
      "Jhandapur BO",
      "LaTTIPUR BO",
      "Marwa BO",
      "Milki BO",
      "Sonbarsa BO",
      "Bihpur SO"
    ]
  },
  "853202": {
    "pincode": "853202",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Akidatpur BO",
      "Bhawanpura BO",
      "Ishapur Chorhar BO",
      "Jamunia BO",
      "Kaluchak Bispuria BO",
      "Lokmanpur BO",
      "Mirjafari BO",
      "Raghopur BO",
      "Telghee BO",
      "Tulsipur Jamunia BO",
      "Karik Bazar SO"
    ]
  },
  "853203": {
    "pincode": "853203",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Balaha BO",
      "Bharakhand Naybas BO",
      "Bharatkhand dyorhi BO",
      "Chakrami BO",
      "Khajraitha BO",
      "Madhurapur BO",
      "Nagarpara BO",
      "Paharpur BO",
      "Raipur BO",
      "Sathishnagar BO",
      "Narayanpur SO Bhagalpur"
    ]
  },
  "853204": {
    "pincode": "853204",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Abhiya bazarBO",
      "Arajpur BO",
      "Baisi jahangira BO",
      "Balia BO",
      "Bhatgama BO",
      "Bhawauipur BO",
      "Bhowadeorhi BO",
      "Chapar BO",
      "Dharahara BO",
      "Dholbazza  BO",
      "Dimaha BO",
      "Gosaigaov BO",
      "Jagatpur BO",
      "Khairpurkadwa BO",
      "KHAGRA",
      "Latra BO",
      "Laxmipurgirdhar BO",
      "Madrauni BO",
      "Makhatakia BO",
      "Maniamore BO",
      "Mohanpur BO",
      "NAGRAH BO",
      "Pakra B.O",
      "Panchgachia Bazar BO",
      "Pratapnager BO",
      "Rangra BO",
      "Sadhwa BO",
      "Sahuparbatta BO",
      "Singhia makandpur BO",
      "Srimaha BO",
      "Tetripakra BO",
      "KATARIYA",
      "Naugachia SO",
      "Kadwadiara BO"
    ]
  },
  "853205": {
    "pincode": "853205",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Bhimdestola BO",
      "Ismailpur BO",
      "Jotgobind BO",
      "Kamlakund BO",
      "Phulakia BO",
      "Raghunitola BO",
      "Suktia BO",
      "Tintangadiara BO",
      "STGoriar SO",
      "Tintanga BO"
    ]
  },
  "854101": {
    "pincode": "854101",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Ayodhyaganj Bazar SO",
      "Balthi Maheshpur BO",
      "Chandpur BO",
      "Chapahari BO",
      "Goriar BO",
      "Khaira BO",
      "MTDevipur BO",
      "Moula Nagar BO",
      "Nandgola BO",
      "Sameli BO",
      "Simra BO",
      "Tikapatti BO"
    ]
  },
  "854102": {
    "pincode": "854102",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Chopra Ram Nagar SO",
      "Akarthapa BO",
      "Bhargama BO",
      "Birnagar BO",
      "Chakmaka BO",
      "Ekraha BO",
      "Harpatti BO",
      "Jankinagar BO",
      "Mangalbar Chiraiya BO",
      "Mohania Chakla BO",
      "Pipra BO",
      "Rampur Tilak BO",
      "Shilanath Rupauli BO",
      "Tetrahi BO"
    ]
  },
  "854103": {
    "pincode": "854103",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Daharia Molls SO",
      "KUMARIPUR BO",
      "MANSAHI BO",
      "SIRNEA BO",
      "TINGACHIA BO"
    ]
  },
  "854104": {
    "pincode": "854104",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Gurubazar SO",
      "BHAIS DIARA BO",
      "BHANDARTAL BO",
      "BISHANPUR BO",
      "BAKIA BO",
      "D BISHANPUR BO",
      "DUMAR BO",
      "HASIMPUR BO",
      "JARLAHI BO",
      "JOTRAMRAI BO",
      "KNGURMELA BO",
      "KANT NAGAR BO",
      "KARHAGOLA BO",
      "LAXMIPUR BO",
      "MARGHIA BO",
      "MARWA BO",
      "PAKAHARA BO",
      "PAWAI BO",
      "ROUNIA BO",
      "SAMDA BO"
    ]
  },
  "854105": {
    "pincode": "854105",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Katihar HO",
      "Katihar Bara Bazar SO",
      "Katihar Colony SO",
      "Katihar Rs SO"
    ]
  },
  "854106": {
    "pincode": "854106",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Katihar Bmp Camp SO",
      "B SIMARIA BO",
      "DALAN BO",
      "DIGHRI BO",
      "SANDALPUR BO"
    ]
  },
  "854107": {
    "pincode": "854107",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Katihar Mill SO",
      "BELWA BO",
      "BUDH NAGAR BO",
      "KURETHA BO",
      "MADHURA BO",
      "SAHJA BO",
      "SAURIYA",
      "SUKHASAN BO"
    ]
  },
  "854108": {
    "pincode": "854108",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Korha SO",
      "BHATWARA BO",
      "DHANETHA BO",
      "JHAGRUCHAK BO",
      "KHERIA BO",
      "MORSANDA BO",
      "MUSAPUR BO",
      "SISIYA BO"
    ]
  },
  "854109": {
    "pincode": "854109",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Katihar Court SO"
    ]
  },
  "854112": {
    "pincode": "854112",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Amdabad SO",
      "Bairia B.O",
      "Bakharganj BO",
      "CHOWKCHAMA BO",
      "Durgapur BO",
      "Gopalpur Diara BO",
      "Paharpur BO",
      "Pani Kamla BO",
      "Pardiara BO"
    ]
  },
  "854113": {
    "pincode": "854113",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Manihari SO",
      "BNDIARA BO",
      "Dilarpur BO",
      "Hemkunj BO",
      "Kantakosh BO",
      "Madarichak BO",
      "MUJWAR BO",
      "M NAWABGANJ BO",
      "Narayanpur BO",
      "RNBOULIA BO",
      "TM Baghar BO"
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
