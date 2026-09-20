"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 400 / 400
================================================================================
- Group: ThirdTest_pincode_group_40_parts_391_to_400
- Assigned PIN Codes: 48 (Range: 854114 to 855117)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_400.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_400.csv & .json
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

PART_ID = "part_400"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-400] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "854114",
  "854115",
  "854116",
  "854117",
  "854201",
  "854202",
  "854203",
  "854204",
  "854205",
  "854301",
  "854302",
  "854303",
  "854304",
  "854305",
  "854306",
  "854311",
  "854312",
  "854315",
  "854316",
  "854317",
  "854318",
  "854325",
  "854326",
  "854327",
  "854328",
  "854329",
  "854330",
  "854331",
  "854332",
  "854333",
  "854334",
  "854335",
  "854336",
  "854337",
  "854338",
  "854339",
  "854340",
  "855101",
  "855102",
  "855105",
  "855106",
  "855107",
  "855108",
  "855113",
  "855114",
  "855115",
  "855116",
  "855117"
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
  "854114": {
    "pincode": "854114",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Pothia SO",
      "Bareta BO",
      "Basantpur BO",
      "Bhangaha BO",
      "Chhohar BO",
      "Falka BO",
      "Pirmokam BO",
      "SALEHPUR BO"
    ]
  },
  "854115": {
    "pincode": "854115",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Semapur Factory SO",
      "Bansgarha BO",
      "Binjee BO",
      "BHBHIR BO",
      "Jagdishpur BO",
      "Kabar BO",
      "Sikat BO",
      "Sujapur BO"
    ]
  },
  "854116": {
    "pincode": "854116",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Mahadevpur SO Katihar",
      "BAINA BO",
      "BASTAUL BO",
      "DHARHAN BO",
      "D BAGCHALLA BO",
      "Gauripur BO",
      "Kehunia BO",
      "MAHUAR BO",
      "MAINA NAGAR BO",
      "Palsa BO",
      "Pranpur BO",
      "Tangore BO"
    ]
  },
  "854117": {
    "pincode": "854117",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Rosna SO",
      "Dilli Diwanganj BO",
      "Neema BO"
    ]
  },
  "854201": {
    "pincode": "854201",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Banaili SO",
      "Balua Kachahari BO",
      "Kala Balua BO",
      "Malinia Diara BO",
      "Ramnagar BO"
    ]
  },
  "854202": {
    "pincode": "854202",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Banmankhi SO",
      "Aurahi Gobindpur BO",
      "Bhatgama BO",
      "Bishanpur Dutt BO",
      "Damgara BO",
      "Deottar BO",
      "Dharhara Millik BO",
      "Dhima BO",
      "Haripur Madi BO",
      "Hingua BO",
      "Kajhi BO",
      "Ladugarh BO",
      "Laukahi Sukhasan BO",
      "MPurandaha BO",
      "Maharajganj BO",
      "Nipania BO",
      "Parsa Hat BO",
      "Rasarh BO"
    ]
  },
  "854203": {
    "pincode": "854203",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Barhara Kothi SO",
      "Bhatottar Chakla BO",
      "Bhatsara BO",
      "Bishanpur Deorhi BO",
      "Dibra BO",
      "Harirahi BO",
      "Kamalpur BO",
      "Latraha BO",
      "Maldiha BO",
      "Mauzampatti BO",
      "Nirpur BO",
      "Orlaha BO"
    ]
  },
  "854204": {
    "pincode": "854204",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Bhawanipur Rajdham SO",
      "Ajhokopa BO",
      "Bahdura BO",
      "Ballia BO",
      "Bardela BO",
      "Barhari BO",
      "Dobha BO",
      "Dumra BO",
      "Durgapur BO",
      "Jabe BO",
      "Jhalari BO",
      "Kukraon BO",
      "Madhavnagar BO",
      "Mehdi BO",
      "Rupauli BO",
      "STBrahmgyani BO",
      "Sahidganj BO",
      "Sondeep BO",
      "Raghunathpur BO"
    ]
  },
  "854205": {
    "pincode": "854205",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Dhamdaha SO",
      "Amari kukron BO",
      "Banshi Purandaha BO",
      "Chandrahi BO",
      "Kuwarigola BO",
      "Sanjhaghat BO",
      "Sripur BO"
    ]
  },
  "854301": {
    "pincode": "854301",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Purnea HO",
      "Bhatta Bazar SO",
      "Purnea Court SO",
      "Purnea DB SO"
    ]
  },
  "854302": {
    "pincode": "854302",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Purnea City SO"
    ]
  },
  "854303": {
    "pincode": "854303",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "BKAsthan BO",
      "Gangali BO",
      "Goasi BO",
      "Harda BO",
      "Khudna Rupaspur BO",
      "Purnea Polyticnic SO",
      "Lalganj BO",
      "Maranga BO",
      "Purnea Aerodram BO",
      "Rahua BO",
      "Sahara BO"
    ]
  },
  "854304": {
    "pincode": "854304",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Bela Rikbaganj BO",
      "Bithnauli BO",
      "Chanka BO",
      "Farkia BO",
      "Ganeshpur BO",
      "Garhiyabalua BO",
      "Jageli BO",
      "Kajha BO",
      "Khemchand BO",
      "Khokha BO",
      "Mirganj BO",
      "Parora BO",
      "Rangpura BO",
      "Rupaspur BO",
      "Sadhubeli BO",
      "Singhiya BO",
      "Srinagar BO",
      "Jhunni Kalan BO",
      "Purnea Chowk SO"
    ]
  },
  "854305": {
    "pincode": "854305",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Khuskibagh SO"
    ]
  },
  "854306": {
    "pincode": "854306",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Sarsi SO",
      "Burhia Dhankatta BO",
      "Champawati BO",
      "Gokulpur BO",
      "Parasmani BO",
      "Zianganj BO",
      "Bahora BO"
    ]
  },
  "854311": {
    "pincode": "854311",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Araria SO",
      "Araria Bairgachhi BO",
      "Bagdahara BO",
      "Balua Deorhi BO",
      "Bansbari BO",
      "Baturbari BO",
      "Bochi BO",
      "Chirah BO",
      "Dabhara BO",
      "Doria Sonapur BO",
      "Dubha BO",
      "Gairki BO",
      "Gaiyari BO",
      "Kakan BO",
      "Pategana BO",
      "RTMohan BO",
      "Rampur Mohanpur BO",
      "Haryabara BO"
    ]
  },
  "854312": {
    "pincode": "854312",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Araria RS SO",
      "Bangama BO",
      "Basaity BO",
      "Baunsi BO",
      "Chandardai BO",
      "Chhatiouna BO",
      "Gidwas BO",
      "Gunwanti BO",
      "Hansa BO",
      "Kamaldaha BO",
      "Karankia BO",
      "Mahsail BO",
      "Majhuwa BO",
      "Mirzapur BO",
      "Mohani BO",
      "Pahsara BO",
      "Parmanpur BO",
      "Rajokhar BO",
      "Tamghatti BO"
    ]
  },
  "854315": {
    "pincode": "854315",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Baisi SO",
      "Amour BO",
      "Asja Mobaiya BO",
      "Bishanpur BO",
      "Channidoria BO",
      "Charaiya BO",
      "Gangarghosh BO",
      "Hafania BO",
      "Kanharia BO",
      "Kharaiya BO",
      "Khoksa BO",
      "Machhatta BO",
      "Makelibalgachhi BO",
      "Routahat BO",
      "Sadipur Bhutaha BO",
      "Sourajabar BO",
      "Talbari BO"
    ]
  },
  "854316": {
    "pincode": "854316",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Bathnaha SO",
      "Amouna BO",
      "Bhadeshwar BO",
      "Bhangahi BO",
      "Sonapur Bazar BO"
    ]
  },
  "854317": {
    "pincode": "854317",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Barsoighat SO",
      "AJHRAIL BO",
      "B BALRAMPUR BO",
      "BAGHWA BO",
      "BALUPARA BO",
      "BATNA BO",
      "BHELAGANJ BO",
      "BIGHOUR BO",
      "DANGOL BO",
      "DHACHNA BO",
      "GANJAN BO",
      "KIRORA BO",
      "KURUM BO",
      "MADHEPUR BO",
      "MAHI NAGAR BO",
      "MAHISHAL BO",
      "MANMAN BO",
      "MARWA BO",
      "MUKURIA BO",
      "PURANAGANJ BO",
      "S WONKAR BO",
      "SUDHANI RS BO",
      "TELTA BO",
      "FATEHPUR BO"
    ]
  },
  "854318": {
    "pincode": "854318",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Forbesganj SO",
      "Amhara BO",
      "Aurahi Hingua BO",
      "B Rampur BO",
      "Dholbazza BO",
      "Haldia Bokra BO",
      "Haripur Dak BO",
      "Kannaili BO",
      "Khairkhan BO",
      "Khawashpur BO",
      "Koshkapur BO",
      "Kushmaha BO",
      "Kushmoul BO",
      "Majhuwa BO",
      "Matiyari BO",
      "Musahari Idgah BO",
      "Palasi Chakardaha BO",
      "Panjarkatta BO",
      "Parwaha BO",
      "Pipra BO",
      "Pothia BO",
      "Ramai BO",
      "Rampur BO",
      "Shankarpur BO",
      "Simarbani BO",
      "Simraha BO",
      "Sourgaon BO",
      "Tamganj BO",
      "Tiraskund BO",
      "SRForbesganj SO"
    ]
  },
  "854325": {
    "pincode": "854325",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Garhbanaili SO",
      "Bagnagar BO",
      "Bhuna BO",
      "Chainpur BO",
      "Chokta BO",
      "Gairah BO",
      "Machhaila BO",
      "Mahalgaon BO",
      "Masuria BO",
      "PTDumaria BO",
      "Paiktola BO",
      "Rampur Kodarkatti BO",
      "Southa BO"
    ]
  },
  "854326": {
    "pincode": "854326",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Gulabbagh SO",
      "Barsauni BO",
      "Belouri BO",
      "Dagarua Hat BO",
      "Mahalbari BO",
      "Mohan kunda BO",
      "Pokharia Benguwabad BO",
      "Siarkhan BO"
    ]
  },
  "854327": {
    "pincode": "854327",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Jalalgarh SO",
      "Ahilgaon BO",
      "Ekamba Nishara BO",
      "Sohangaon BO",
      "Tarauna BO"
    ]
  },
  "854328": {
    "pincode": "854328",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Jogbani SO",
      "Maheshwari BO"
    ]
  },
  "854329": {
    "pincode": "854329",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Jokihat SO",
      "Baharbari BO",
      "Bara Istambarar BO",
      "Bihari BO",
      "Chakaihat BO",
      "Dalmalpur BO",
      "Jahanpur BO",
      "Kesharra BO",
      "Kursail BO",
      "Mainahat BO",
      "Matiyari BO",
      "Pakari BO",
      "Patharbari BO",
      "Sohandarhat BO",
      "Tarbi BO"
    ]
  },
  "854330": {
    "pincode": "854330",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Kasba SO Purnia",
      "Adhang BO",
      "Bara Idagah BO",
      "Begumpur BO",
      "Betouna BO",
      "Bharmara BO",
      "Bochgaon BO",
      "Gurahi BO",
      "Jhawari BO",
      "KullaKhas BO",
      "Madarghat BO",
      "Mahamadia BO",
      "Majgama BO",
      "Majhuwakalan BO",
      "Matkopa BO",
      "Mathour BO",
      "Tolikola BO"
    ]
  },
  "854331": {
    "pincode": "854331",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Kursakanta SO",
      "Chiknihat BO",
      "Haldharahat BO",
      "Jamua BO",
      "Megra Mehndipur BO",
      "Sohagmare BO",
      "Tirakhardah BO"
    ]
  },
  "854332": {
    "pincode": "854332",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Kuari SO",
      "Amgachhi BO",
      "Bathraha Ashabhag BO",
      "Pahunsi BO"
    ]
  },
  "854333": {
    "pincode": "854333",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Madanpur SO Araria",
      "Balua kaliaganj BO",
      "Bardahahat BO",
      "Barmasia Simarbani BO",
      "Bhirbhiri BO",
      "Bhojpur BO",
      "Dehati BO",
      "Dharamaganj BO",
      "Gamharia BO",
      "Kankhudia BO",
      "Kujari BO",
      "Maldwar BO",
      "Palasi BO",
      "Pararia BO",
      "Sikti BO",
      "Urlaha BO"
    ]
  },
  "854334": {
    "pincode": "854334",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Maryganj SO",
      "Bagulaha BO",
      "Belsara BO",
      "Bikash Bhargama BO",
      "Dumuria Bisstaria BO",
      "Indrapur Barhara BO",
      "Jagta BO",
      "Jaynagar BO",
      "KBaijnathpur BO",
      "Khajurihat BO",
      "Kushkapur BO",
      "Madhulata BO",
      "Mahthawabazar BO",
      "Manullah patti BO",
      "Maujaha BO",
      "Mirjapur Kothi BO",
      "Pachira BO",
      "Paikpar BO",
      "Parihari BO",
      "Raghunathpur BO",
      "Shekhapura BO",
      "Sirsia Kalan BO"
    ]
  },
  "854335": {
    "pincode": "854335",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Narpatganj SO",
      "Barhara BO",
      "Bhagwa Deviganj BO",
      "Fatehpur BO",
      "Gokhlapur BO",
      "Khagra BO",
      "Madhura BO",
      "Mirdaul BO",
      "Nathpur BO",
      "Pithoura BO",
      "Rajganj BO"
    ]
  },
  "854336": {
    "pincode": "854336",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Achra BO",
      "Basmatia BO",
      "Bela Basmatia BO",
      "Bhawanipur BO",
      "Ghurna BO",
      "Manikpur BO",
      "Nawabganj SO Araria"
    ]
  },
  "854337": {
    "pincode": "854337",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Purnea Division",
    "offices": [
      "Ranipatra SO",
      "Belwa BO",
      "Bhogabhatgama BO",
      "Hasanganj BO",
      "Kalsar BO",
      "Rautara BO",
      "Gaura BO"
    ]
  },
  "854338": {
    "pincode": "854338",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Bhimnagar Bairrage SO",
      "Saheban BO"
    ]
  },
  "854339": {
    "pincode": "854339",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Balua Bazar SO",
      "Balbhadrapur BO",
      "Bhimpur BO",
      "Lakshminia BO",
      "Matiayari BO",
      "Sanskrit Nirmali BO",
      "Sitapur BO"
    ]
  },
  "854340": {
    "pincode": "854340",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Saharsa Division",
    "offices": [
      "Birpur SO Supaul",
      "Banailipatti BO",
      "Basantpur BO"
    ]
  },
  "855101": {
    "pincode": "855101",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Bahadurganj SO Kishanganj",
      "Baigna BO",
      "Bairia BO",
      "Bangama BO",
      "BHORADAH",
      "Bhittabari BO",
      "Bibiganj BO",
      "Bishanpur BO",
      "Dhangarha BO",
      "Dhantola BO",
      "Dighalbank BO",
      "Fatehpur BO",
      "Jhala BO",
      "KORHOBARI BO",
      "Laucha BO",
      "Lohargarha BO",
      "M BATHNA BO",
      "Mangura BO",
      "Matiari BO",
      "NISHANDRA BO",
      "Rupani BO",
      "SOMESHWAR BO",
      "TERHAGACHHI BO",
      "Thiraniganj BO",
      "Tulsia BO"
    ]
  },
  "855102": {
    "pincode": "855102",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Barsoi SO",
      "Awadpur BO",
      "Chhoghra BO",
      "Dasgram BO",
      "GOWALTOLI BO",
      "Hat Balrampur BO",
      "Jalki BO",
      "KHIDIRPUR BO",
      "Mathurapur BO",
      "Nimoul BO",
      "Porla BO",
      "Sankola BO",
      "Shivananpur BO",
      "SHITALMANI BO"
    ]
  },
  "855105": {
    "pincode": "855105",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Durgaganj SO",
      "BABHANI BO",
      "BHARRI BO",
      "CHANDPUR BO",
      "CHOWKI HARIPUR BO",
      "DAWASAI BO",
      "PARVELI BO",
      "SAGRATH BO"
    ]
  },
  "855106": {
    "pincode": "855106",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Galgalia SO"
    ]
  },
  "855107": {
    "pincode": "855107",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Kishanganj SO",
      "ALTAHAT BO",
      "ASJA BO",
      "BAGALBARI BO",
      "BELWA BO",
      "CHAKLA GHAT BO",
      "DHUSMALHAT BO",
      "GMBATHNA BO",
      "GANGI BO",
      "KANHAIYABARI HAT BO",
      "KASHIBARI HAT BO",
      "KHARUDAH BO",
      "KUTTI GOLA BO",
      "NATUA PARA BO",
      "PATKOI HAT BO",
      "PIYAJEE BO",
      "RAIPUR BO",
      "RUNKI BO",
      "SINGHIA BO",
      "SIRSEE BO",
      "TEOSA BO"
    ]
  },
  "855108": {
    "pincode": "855108",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Kishanganj Bazar SO",
      "BBBHARATPUR BO",
      "BHOLMARA BO",
      "JIA POKHAR BO",
      "KHODAGANJ BO",
      "MEHARGANJ BO",
      "PADAMPUR BO",
      "POWAKHALI BO",
      "SARAIKURI BO",
      "TARABARI BO"
    ]
  },
  "855113": {
    "pincode": "855113",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Salmari SO",
      "Azamnagar BO",
      "Balia Belon BO",
      "BALTAR BO",
      "Belbari BO",
      "Dhumnagar BO",
      "Jhawa BO",
      "Kharsouta BO",
      "KURHELA BOBRA BO",
      "Nimoul BO",
      "Sadapur BO",
      "SHIHIPUR BO",
      "Siktia BO",
      "Teghra BO"
    ]
  },
  "855114": {
    "pincode": "855114",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Sonaili SO",
      "ARIHANA BO",
      "Bijaili BO",
      "Dandkhora BO",
      "Dumaria BO",
      "Kadwa BO",
      "MPGARH BO",
      "RAGHELI BO"
    ]
  },
  "855115": {
    "pincode": "855115",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Sontha SO",
      "Angarhat BO",
      "Dhanpatganj BO",
      "Dogharia BO",
      "Haldikhora BO",
      "Kochadhaman BO",
      "Mirpur BO",
      "MAUDHA BO",
      "Shishabari BO",
      "Singhari BO"
    ]
  },
  "855116": {
    "pincode": "855116",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Thakurganj SO",
      "Bhawaniganj BO",
      "Churli BO",
      "Gunjanmari BO",
      "Hulhuli BO",
      "Jirangachhi BO",
      "Nainagachhi BO",
      "Pathamari BO",
      "Patharia BO",
      "Piprithan BO",
      "Ruidhasa BO"
    ]
  },
  "855117": {
    "pincode": "855117",
    "circle": "Bihar Circle",
    "region": "East Region, Bhagalpur",
    "division": "Divisional Office Katihar",
    "offices": [
      "Taiyabpur SO",
      "Chanamana BO",
      "CHATTAR GACHHI BO",
      "Daluahat BO",
      "Khatia Pichhia BO",
      "MARIYA BO",
      "Mirjapur BO",
      "PThakurbari BO",
      "Pahar Katta BO",
      "Panasihat BO",
      "Poth RS BO",
      "SHITALPUR BO"
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
