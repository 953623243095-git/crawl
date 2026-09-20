"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 391 / 400
================================================================================
- Group: ThirdTest_pincode_group_40_parts_391_to_400
- Assigned PIN Codes: 48 (Range: 841234 to 841427)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_391.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_391.csv & .json
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

PART_ID = "part_391"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-391] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "841234",
  "841235",
  "841236",
  "841237",
  "841238",
  "841239",
  "841240",
  "841241",
  "841242",
  "841243",
  "841244",
  "841245",
  "841286",
  "841287",
  "841301",
  "841302",
  "841305",
  "841311",
  "841312",
  "841313",
  "841316",
  "841401",
  "841402",
  "841403",
  "841404",
  "841405",
  "841406",
  "841407",
  "841408",
  "841409",
  "841410",
  "841411",
  "841412",
  "841413",
  "841414",
  "841415",
  "841416",
  "841417",
  "841418",
  "841419",
  "841420",
  "841421",
  "841422",
  "841423",
  "841424",
  "841425",
  "841426",
  "841427"
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
  "841234": {
    "pincode": "841234",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Darauli SO",
      "Amarpur BO",
      "Balahu BO",
      "Bishwania BO",
      "Gauri BO",
      "Keotalia BO",
      "Krishnapali BO",
      "Sareyarampur BO"
    ]
  },
  "841235": {
    "pincode": "841235",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Done SO",
      "Belaon BO",
      "Draili Mathia BO",
      "Harnatar BO",
      "Kanhauli BO",
      "Kumhatti BO",
      "Mathia BO",
      "Murakhap BO",
      "Murera BO",
      "Tariwani BO",
      "Netwar BO"
    ]
  },
  "841236": {
    "pincode": "841236",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Hasanpura SO",
      "Aranda BO",
      "Bhikhpur Bhgwanpur BO",
      "Dhanutihata BO",
      "Rajanpura BO",
      "Rampur Ke Tola BO",
      "Siswan Kala BO"
    ]
  },
  "841237": {
    "pincode": "841237",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Hussainganj SO",
      "Ballia BO",
      "Bhikhpur BO",
      "Hathura BO",
      "Machakna BO"
    ]
  },
  "841238": {
    "pincode": "841238",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Mahrajganj SO Siwan",
      "Balliapokhra BO",
      "S Bangra BO",
      "Bishunpura BO",
      "Chhotkateghra BO",
      "Deoria BO",
      "Gohpur Bajrahia BO",
      "Hahwa BO",
      "Jigrawan BO",
      "Karsaut BO",
      "Kasdeora Bangra BO",
      "Kothua Sarangpur BO",
      "Pakwalia BO",
      "Pasnauli BO",
      "Rajapur BO",
      "Rishura BO",
      "Rukundipur BO"
    ]
  },
  "841239": {
    "pincode": "841239",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Mairwa SO",
      "Akolhi BO",
      "Anugrhnagar BO",
      "Bargaon BO",
      "Belaur BO",
      "Belauri BO",
      "Bilaspur BO",
      "Bishunpura Bazar BO",
      "Bishwar BO",
      "Bkulari BO",
      "Daraila BO",
      "English BO",
      "Jamuaon BO",
      "Krom BO",
      "Langarpura BO",
      "Lebhri BO",
      "Punak BO",
      "Sarhrwa BO",
      "Sematar BO",
      "Seotapur BO",
      "Sohgra BO",
      "Talli BO"
    ]
  },
  "841240": {
    "pincode": "841240",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Mahuwal Mahal SO",
      "Chandparsa BO",
      "Merhi BO",
      "Tarwaparsa BO"
    ]
  },
  "841241": {
    "pincode": "841241",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Pachrukhi SO",
      "Badram BO",
      "Champ BO",
      "Gambhria BO",
      "Itwa Bartwalia BO",
      "Jasauli BO",
      "Nainpura BO",
      "Sarauti BO",
      "Sonbarsa BO"
    ]
  },
  "841242": {
    "pincode": "841242",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Paterhi SO",
      "Choiapali BO",
      "Paterha BO",
      "Ratanparauli BO"
    ]
  },
  "841243": {
    "pincode": "841243",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Sahpur SO",
      "BPEkdanga BO",
      "Barkamanjha BO",
      "Chainpur Hata BO",
      "Chhotka Manjha BO",
      "Jagdishpur Kothi BO",
      "Kabirpur BO",
      "Keelpur BO",
      "Khalwa BO",
      "Nautan BO",
      "Pachphera BO",
      "Semaria BO",
      "Sirisia BO"
    ]
  },
  "841244": {
    "pincode": "841244",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Takipur SO",
      "Balaun BO",
      "Bhikha Bandh Satjora BO",
      "Pipra BO",
      "Ramgarh BO",
      "Rasulpur Chakri BO",
      "Siktia BO"
    ]
  },
  "841245": {
    "pincode": "841245",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Zeradei SO",
      "Banthu Sriram BO",
      "Bardaha BO",
      "Barhulia BO",
      "Bhaisakhal BO",
      "Bharuli BO",
      "Chhitanpur BO",
      "Kpmishrauli BO",
      "Makariyar BO",
      "Mania BO",
      "Miyan Ke Bhatkan BO",
      "Pachokhar BO",
      "Piprahiya BO",
      "Ruiyabangra BO",
      "Sanjalpur BO",
      "Shiopur Sakra BO",
      "Sisahni BO",
      "Thepaha Bazar BO",
      "Titra BO"
    ]
  },
  "841286": {
    "pincode": "841286",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bakerganj SO",
      "Alapur BO",
      "Harihans BO",
      "Rafipur BO",
      "Sahuli BO",
      "Telkathu BO"
    ]
  },
  "841287": {
    "pincode": "841287",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Asaon SO",
      "Kandhpakar BO",
      "Kashila BO",
      "Pihuli BO"
    ]
  },
  "841301": {
    "pincode": "841301",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Chapra HO",
      "Bhagwan Bazar SO",
      "District Board SO",
      "Jagdam College SO",
      "Mauna SO",
      "Rajendra College SO Saran",
      "Daulatganj BO"
    ]
  },
  "841302": {
    "pincode": "841302",
    "circle": "Bihar circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Paharpur BO",
      "Purwari Telpa SO",
      "Gheghta BO",
      "Mahamda BO",
      "Tari BO",
      "Pirauna BO",
      "Gurukul Mehiya BO",
      "Sarha BO"
    ]
  },
  "841305": {
    "pincode": "841305",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Revelganj SO",
      "Enai BO",
      "Jigna Parsa BO",
      "Majhanpura BO",
      "Sengar Tola BO",
      "Sirsia BO",
      "Brahampur BO"
    ]
  },
  "841311": {
    "pincode": "841311",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Garkha SO",
      "Bhainsmara BO",
      "Gohpur BO",
      "Katsa BO",
      "Kewani BO",
      "Madarpur BO",
      "Mohammadpur BO",
      "Parsajogini BO",
      "Rampur BO",
      "Saidsarai BO",
      "Saraibuksh BO",
      "Sobhepur BO"
    ]
  },
  "841312": {
    "pincode": "841312",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Gariba Tola SO",
      "Khawaspur BO",
      "Sahebzadatola BO",
      "Sitabdiara BO"
    ]
  },
  "841313": {
    "pincode": "841313",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Manjhi SO",
      "Dumri BO",
      "GDurgapur BO",
      "Ghorhat Mathia BO",
      "Nandpur BO",
      "Narpaliya BO",
      "Sonbarsa BO"
    ]
  },
  "841316": {
    "pincode": "841316",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Naini SO Saran",
      "Fakuli BO",
      "Magaidih BO"
    ]
  },
  "841401": {
    "pincode": "841401",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Amnour SO",
      "Amnour Harnarain BO",
      "Baldiha BO",
      "Basantpur Bangla BO",
      "Bhawalpur BO",
      "Deorhi BO",
      "GNParshurampur BO",
      "Jhakhra BO",
      "Karanpura BO",
      "Narayanpur BO",
      "Nautan BO",
      "PBChapra BO",
      "Puraina BO",
      "Sareya Basant BO"
    ]
  },
  "841402": {
    "pincode": "841402",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Aphar SO",
      "Bheldi BO",
      "Dharhara Kala BO",
      "Dholahi Kaithal BO",
      "Pachlakh BO",
      "Pachrukhi BO"
    ]
  },
  "841403": {
    "pincode": "841403",
    "circle": "Bihar circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Berui BO",
      "Bhatkeshri BO",
      "Baniapur SO",
      "Chorauwa BO",
      "Dhanaw BO",
      "Dhangarha BO",
      "GPiparpanti BO",
      "HKarah BO",
      "Hariharpur BO",
      "KManohar BO",
      "Kamta BO",
      "Lauwa Kala BO",
      "Mirzapur Khurd Lauwa BO",
      "Murarpur BO",
      "Nagdiha BO",
      "Puchhari BO",
      "Saraya BO",
      "Satua BO",
      "Sohai Shahpur BO",
      "Ushti BO"
    ]
  },
  "841404": {
    "pincode": "841404",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bagaura SO",
      "KTBharauli BO"
    ]
  },
  "841405": {
    "pincode": "841405",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Barauli SO Gopalganj",
      "Achhaibar Pipra BO",
      "BASonbarsa BO",
      "BRampur BO",
      "Batardey BO",
      "Devapur BO",
      "Hassanpur Mathiya BO",
      "Kahla BO",
      "Kamalpur BO",
      "Khajuri BO",
      "Lohijara BO",
      "MBiraicha BO",
      "Nawada BO",
      "Neori BO",
      "Rupanchhap BO",
      "Sadauwan BO",
      "Salempur BO",
      "Sareya Narendra BO",
      "Sikatiya Sridhar Sahi BO"
    ]
  },
  "841406": {
    "pincode": "841406",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Basantpur SO Siwan",
      "Baghi BO",
      "Baijubarhoga BO",
      "Basaowan BO",
      "Chorauli BO",
      "Chorma BO",
      "Hussaipurnand BO",
      "Jagatpur BO",
      "Kauria BO",
      "Kherwa BO",
      "Mohmda Bazar BO",
      "Molnapur BO",
      "Sahrkola BO",
      "Sekhpura BO",
      "Sohilpatti BO",
      "Sughri BO"
    ]
  },
  "841407": {
    "pincode": "841407",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bishunpura SO",
      "Dangsi BO",
      "Hakam BO",
      "Kashi Tengrahi BO",
      "Khajuhatti BO",
      "Maharani BO",
      "Mahuwa BO",
      "Mohammadpur BO",
      "Reotith BO",
      "Sher BO",
      "Shyampur BO",
      "Talimapur BO"
    ]
  },
  "841408": {
    "pincode": "841408",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bhagwanpur Hat SO",
      "Barhipurgop BO",
      "Barkagaowan BO",
      "Bhikhampur BO",
      "Hulesra BO",
      "Sahsrawan BO",
      "Sondhani BO"
    ]
  },
  "841409": {
    "pincode": "841409",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Dighwdubauli SO",
      "Baikunthpur BO",
      "Usribazar BO"
    ]
  },
  "841410": {
    "pincode": "841410",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Dumarshan Bangra SO",
      "Bellour BO",
      "Brit Bhagwanpr BO",
      "Chakia BO",
      "Dubauli BO",
      "Pyarepur BO",
      "Satjora Bazar BO",
      "Sonbarsa BO"
    ]
  },
  "841411": {
    "pincode": "841411",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Ishuwapur SO",
      "Agauthar Nanda BO",
      "Bhithi Sahabuddin BO",
      "Chandpurwa BO",
      "Kerwa BO",
      "Maricha BO",
      "Ram Chaura BO",
      "Sahawa BO",
      "Salempur BO",
      "Shyam Kauria BO",
      "Terha BO"
    ]
  },
  "841412": {
    "pincode": "841412",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Jalalpur Bazar SO",
      "Ashok Nagar BO",
      "Basdila BO",
      "Dhoopnagar Dhobawal BO",
      "GSBangra BO",
      "Kahi BO",
      "Kishunpur BO",
      "Kotheya BO",
      "Mangolapur BO",
      "Nawada BO",
      "Sakaddi BO",
      "Sawari Buxi Jee BO"
    ]
  },
  "841413": {
    "pincode": "841413",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Jamobazar SO",
      "Belsand BO",
      "Hatimpur BO",
      "Jogapur Kothi BO",
      "Madhopur BO",
      "Paltuhatta BO",
      "Sirisia BO",
      "Sultanpur Kala BO"
    ]
  },
  "841414": {
    "pincode": "841414",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Khaira SO Saran",
      "Afaur BO",
      "Amar Chapra BO",
      "Badlu Tola BO",
      "Chanchaura BO",
      "Kakadhiya BO",
      "Kanar Hariharpur BO",
      "Manpur BO",
      "Meera Mushehri BO",
      "Mushehri BO",
      "Rampur Kala BO"
    ]
  },
  "841415": {
    "pincode": "841415",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Khodaibagh SO",
      "Bhualpur BO",
      "Ferusa BO",
      "Madhopur BO",
      "Mubarakpur BO",
      "Olahanpur BO",
      "Rasidpur BO",
      "Sadhpur BO"
    ]
  },
  "841416": {
    "pincode": "841416",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Kishunpura SO",
      "Chimanpura BO",
      "Dumra BO",
      "Khawaspur BO",
      "Lakari BO",
      "Parauli BO",
      "Safiabad BO",
      "Sarif Jalalpur BO"
    ]
  },
  "841417": {
    "pincode": "841417",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Masrakh SO",
      "Arna BO",
      "Bahrauli BO",
      "Balsohi BO",
      "Chhapia BO",
      "Dhenuki BO",
      "Dumari Chhapia BO",
      "Jaithar BO",
      "Kawalpura BO",
      "Nawada BO",
      "Pakari BO",
      "Rasauli BO",
      "Sagar Sultanpur BO",
      "Sherukaha BO"
    ]
  },
  "841418": {
    "pincode": "841418",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Marhowrah HO"
    ]
  },
  "841419": {
    "pincode": "841419",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Mirzapur SO Saran",
      "Atta BO",
      "Awari BO",
      "Bardahiya BO",
      "Bhaluhi BO",
      "Hussepur BO",
      "Ishrauli BO",
      "Rasulpur BO",
      "Senduari BO",
      "Silhauri BO",
      "Tarwar BO",
      "Tejpurwa BO"
    ]
  },
  "841420": {
    "pincode": "841420",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Rajapatti SO",
      "Dabchhu BO",
      "Faizullahpur BO",
      "Gamhari BO",
      "Gopalpur BO",
      "Harpurjan BO",
      "Katalpur BO",
      "Matiyari BO",
      "Paharpur BO"
    ]
  },
  "841421": {
    "pincode": "841421",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Sarhwara SO",
      "Amardah BO",
      "Gamahria BO",
      "Rampur Atauli BO",
      "Semri BO",
      "Sikati Bhikham BO",
      "Sonauli BO"
    ]
  },
  "841422": {
    "pincode": "841422",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Sahajitpur SO",
      "Bankat BO",
      "Hafijpur BO",
      "Kolhua BO",
      "Madarpur BO",
      "Pipra BO",
      "Sarmi BO",
      "Sisai BO"
    ]
  },
  "841423": {
    "pincode": "841423",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Sidhwalia SO",
      "Amarpura BO",
      "Barhima BO",
      "Dumaria BO",
      "Harpur Tengrahi BO",
      "Jhanjhawa Mohamadpur BO",
      "Sorahiya BO",
      "Tekniwas BO"
    ]
  },
  "841424": {
    "pincode": "841424",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Saran Division",
    "offices": [
      "Taraiya SO",
      "Ardewa BO",
      "Belhari BO",
      "Bhagwatpur BO",
      "Bhalua Bhikhari BO",
      "Bhatgai BO",
      "Bhorha BO",
      "Dumri BO",
      "Gawandri BO",
      "Lauwa BO",
      "Moreya BO",
      "Pachbhinda BO",
      "Pachraur BO",
      "Parauna BO",
      "Pokhrera BO",
      "Rajdhani BO",
      "Rampur Rudra BO",
      "Ushri Chandpura BO"
    ]
  },
  "841425": {
    "pincode": "841425",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bathubazar SO",
      "Bhagwatparsa BO",
      "Chhitauna BO",
      "Kalyanpur BO",
      "Phulwaria BO",
      "Songharwa BO"
    ]
  },
  "841426": {
    "pincode": "841426",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Bhore SO",
      "Ahiyapur BO",
      "Bankata Jagirdari BO",
      "Chakarwakhas BO",
      "Chhitauna BO",
      "DNarendra BO",
      "Hussepur BO",
      "Jagtauli BO",
      "Jignadubey BO",
      "Lamichaur BO",
      "Matiyari BO",
      "Musahri Bazar BO",
      "Rudalpur BO",
      "Sisai BO"
    ]
  },
  "841427": {
    "pincode": "841427",
    "circle": "Bihar Circle",
    "region": "Muzaffarpur Region",
    "division": "Siwan Division",
    "offices": [
      "Garhmanjha SO",
      "Bhaisahi BO",
      "Dharamparsa BO",
      "Gausia BO",
      "KSrirampur BO",
      "Koini BO",
      "Manjhariya BO",
      "Mungarha BO",
      "Nimuiya BO",
      "Parsurampur BO",
      "Pathra BO",
      "Purdiltola BO",
      "Rachhopali BO"
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
