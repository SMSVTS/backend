
from django.conf import settings
from db_interface.queries import *
from db_interface.execute import *
from django.views.decorators.csrf import csrf_exempt
from utilities.constants import *
from datetime import datetime,timedelta
from smsvts_flower_market.globals import *
import json,uuid,math
import base64
import os
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.contrib.auth.hashers import make_password
from fpdf import FPDF, HTMLMixin,FontFace
import os
import requests
import xml.etree.ElementTree as ET
import hashlib
from django.http import JsonResponse
from django.http import HttpResponse
from django.template.loader import render_to_string
import os
from fpdf import FPDF



# def generate_tamil_pdf(output_directory, file_name):
#     """
#     Generate a PDF with Tamil text and save it to the specified output directory.
    
#     :param output_directory: The directory where the PDF should be saved.
#     :param file_name: The name of the output PDF file.
#     :return: The path where the PDF is saved.
#     """
#     # Define PDF class
#     class PDF(FPDF):
#         def __init__(self):
#             super().__init__()

#     # Initialize PDF
#     pdf = PDF()

#     # Path to the Tamil-compatible font (Noto Sans Tamil downloaded from Google Fonts)
#     font_path = os.path.join(os.getcwd(), "fonts", "NotoSansTamil-Regular.ttf")  # Ensure this is the correct path

#     # Check if the font file exists
#     if not os.path.exists(font_path):
#         raise FileNotFoundError(f"Font file not found at {font_path}. Please make sure the font is in the correct location.")

#     # Add the Tamil font to FPDF
#     pdf.add_font('NotoSansTamil', '', font_path, uni=True)

#     # Add a page
#     pdf.add_page()

#     # Set the font for Tamil text
#     pdf.set_font('NotoSansTamil', '', 12)

#     # Tamil content
#     tamil_content = "தமிழ்நாட மலர்கள் உற்பத்தியாளர்கள் விவசாயிகள் சங்கம்"

#     # Write the Tamil content to the PDF
#     pdf.cell(0, 10, tamil_content, ln=True, align='C')

#     # Ensure the output directory exists
#     if not os.path.exists(output_directory):
#         os.makedirs(output_directory)

#     # Save the PDF
#     output_path = os.path.join(output_directory, file_name)
#     pdf.output(output_path)

#     return output_path

# # Example of how to call the function
# output_pdf_path = generate_tamil_pdf(output_directory="output", file_name="output.pdf")



#All perfect with table and the details
# def generate_pdf_with_table(table_data, trader_details, output_directory, file_name):
#     """
#     Generate a PDF with a well-aligned table, trader details, and totals.
#     """
#     class PDF(FPDF):
#         def header(self):
#             # Tamil heading
#             self.set_font('NotoSansTamil', 'B', 14)
#             self.cell(0, 10, 'சத்தி மலர் சாகுபடி விவசாயிகள் தலைமை சங்கம்', ln=True, align='C')
#             self.cell(0, 10, 'சத்தியமங்கலம்', ln=True, align='C')
#             self.ln(10)

#         def footer(self):
#             # Page footer
#             self.set_y(-15)
#             self.set_font('Arial', 'I', 8)
#             self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

#     # Initialize PDF
#     pdf = PDF()

#     # Tamil font paths
#     font_path = os.path.join(os.getcwd(), "fonts", "NotoSansTamil-Regular.ttf")
#     bold_font_path = os.path.join(os.getcwd(), "fonts", "NotoSansTamil-Bold.ttf")

#     # Verify font files exist
#     if not os.path.exists(font_path) or not os.path.exists(bold_font_path):
#         raise FileNotFoundError("Tamil font files not found.")

#     # Add Tamil font
#     pdf.add_font('NotoSansTamil', '', font_path, uni=True)
#     pdf.add_font('NotoSansTamil', 'B', bold_font_path, uni=True)

#     # Add a page
#     pdf.add_page()
#     pdf.set_font('NotoSansTamil', '', 12)

#     # Trader details
#     pdf.cell(100, 10, f"வியாபாரி: {trader_details['name']}", ln=0, align='L')
#     pdf.cell(0, 10, f"தேதி: {trader_details['date']}", ln=1, align='R')
#     pdf.ln(10)

#     # Dynamically calculate column widths
#     headers = ["காலம்", "விளக்கம்", "செலவுத்தொகை", "வருவாய்தொகை"]
#     col_widths = [pdf.get_string_width(header) + 10 for header in headers]
    
#     for row in table_data:
#         for idx, header in enumerate(headers):
#             content_width = pdf.get_string_width(str(row.get(header, ""))) + 10
#             col_widths[idx] = max(col_widths[idx], content_width)
    
#     total_width = sum(col_widths)
#     page_width = pdf.w - 20  # Considering margins
#     if total_width > page_width:
#         scale_factor = page_width / total_width
#         col_widths = [width * scale_factor for width in col_widths]

#     # Table headers
#     pdf.set_font('NotoSansTamil', 'B', 12)
#     for idx, header in enumerate(headers):
#         pdf.cell(col_widths[idx], 10, header, border=1, align='C')
#     pdf.ln()

#     # Table rows
#     pdf.set_font('NotoSansTamil', '', 12)
#     for row in table_data:
#         for idx, header in enumerate(headers):
#             cell_data = str(row.get(header, ""))
#             pdf.cell(col_widths[idx], 10, cell_data, border=1, align='C')
#         pdf.ln()

#     # Ensure output directory exists
#     if not os.path.exists(output_directory):
#         os.makedirs(output_directory)

#     # Save PDF
#     output_path = os.path.join(output_directory, file_name)
#     pdf.output(output_path)

#     return output_path

# font_path = "fonts/NotoSansTamil-Regular.ttf"
# bold_font_path = "fonts/NotoSansTamil-Bold.ttf"









# def generate_pdf_with_table(table_data, trader_details, output_directory, file_name):
#     """
#     Generate a PDF with a well-aligned table, trader details, and totals.
#     """
#     class PDF(FPDF):
#         def header(self):
#             # Tamil heading with correct font
#             self.set_font('NotoSansTamil-Bold', '', 14) 
#             self.cell(0, 10, 'சத்தி மலர் சாகுபடி விவசாயிகள் தலைமை சங்கம்', ln=True, align='C')
#             self.cell(0, 10, 'சத்தியமங்கலம்', ln=True, align='C')
#             self.ln(10)

#         def footer(self):
#             # Page footer
#             self.set_y(-15)
#             self.set_font('Arial', 'I', 8)
#             self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

#     # Initialize PDF
#     pdf = PDF()

#     # Tamil font paths
#     font_path = os.path.join(os.getcwd(), "fonts", "NotoSansTamil-Regular.ttf")
#     bold_font_path = os.path.join(os.getcwd(), "fonts", "NotoSansTamil-Bold.ttf")

#     # Verify font files exist
#     if not os.path.exists(font_path) or not os.path.exists(bold_font_path):
#         raise FileNotFoundError("Tamil font files not found.")

#     # Add Tamil fonts with correct names
#     pdf.add_font('NotoSansTamil-Regular', '', font_path, uni=True)
#     pdf.add_font('NotoSansTamil-Bold', '', bold_font_path, uni=True)

#     # Add a page
#     pdf.add_page()
#     pdf.set_font('NotoSansTamil-Regular', '', 12)

#     # Trader details
#     pdf.cell(100, 10, f"வியாபாரி: {trader_details['name']}", ln=0, align='L')
#     pdf.cell(0, 10, f"தேதி: {trader_details['date']}", ln=1, align='R')
#     pdf.ln(10)

#     # ... (rest of the code remains the same)

#     # Save PDF
#     output_path = os.path.join(output_directory, file_name)
#     pdf.output(output_path)

#     return output_path

# @csrf_exempt
# def generate_pdf(request):
#     try:
#         if request.method == "POST":
#             data = json.loads(request.body)
#             utc_time = datetime.utcnow()

#             request_header = request.headers
#             auth_token = request_header["Authorization"]
#             access_token = data["access_token"]

#             state,msg,user = authorization(auth_token=auth_token,access_token=access_token)
#             if state == False:
#                 return JsonResponse(msg, safe=False)
            
            
#             # table_data = [
#             # {"Column 1": "Row 1, Col 1", "Column 2": "Row 1, Col 2", "Column 3": "Row 1, Col 3", "Column 4": "Row 1, Col 4"},
#             # {"Column 1": "Row 2, Col 1", "Column 2": "Row 2, Col 2", "Column 3": "Row 2, Col 3", "Column 4": "Row 2, Col 4"},
#             # {"Column 1": "Row 3, Col 1", "Column 2": "Row 3, Col 2", "Column 3": "Row 3, Col 3", "Column 4": "Row 3, Col 4"},

            
#             # ]
            
#             # trader_details = {
#             # "name": "John Doe",
#             # "phone": "1234567890",
#             # "date": "2025-01-21",
#             # "code": "ABC123",
#             # "total": "5000",
#             # "total_quantity": "100"
#             # }

#             trader_details = data.get("trader_details")
#             table_data = data.get("table_data")

#             # trader_name = trader_details['name']
#             data_uniq_id = str(uuid.uuid4())
#             output_directory = 'media/pdf/'
#             file_name = f"{data_uniq_id}.pdf"

#             # if trader_details == []:
#             #     return JsonResponse({
#             #         "status": 400,
#             #         "action_status": "error",
#             #         "message": "Missing trader details"
#             #     }, safe=False)
                
#             if table_data == "" or table_data == None:
#                 return JsonResponse({
#                     "status": 400,
#                     "action_status": "error",
#                     "message": "Missing table details"
#                 }, safe=False)
            
#             result = generate_pdf_with_table(table_data,trader_details,output_directory,file_name)

#             # result = generate_tamil_pdf('media/pdf/')

#             # search_existing_data = """select * from pdf_table where 1=1"""
#             # get_existing_data = search_all(search_existing_data)

#             # if len(get_existing_data) !=0:
#             #     delete_existing_data = "delete from pdf_table"
#             #     delete_result = django_execute_query(delete_existing_data)
                
#             # insert_query = """insert into pdf_table (data_uniq_id, pdf_path, created_date, modified_date) values ('{data_uniq_id}', '{pdf_path}', '{created_date}', '{modified_date}')""".format(data_uniq_id=data_uniq_id,pdf_path=result,created_date=utc_time,modified_date=utc_time)
#             # execute = django_execute_query(insert_query)

#             if result != 0:
#                 return JsonResponse({"status": 200,'action_status':'success', "message": "Pdf generated successfully", "pdf_path":result}, safe=False)
#             else:
#                 return JsonResponse({"status": 400,'action_status':'error', "message": "Failed to generate pdf"}, safe=False)
#         else:
#             return JsonResponse({"status": 400,'action_status':'error', "message": "Invalid request method"}, safe=False)
#     except Exception as Err:
#         response_exception(Err)
#         return JsonResponse(str(Err), safe=False)
    
def generate_pdf_with_template(table_data, output_directory, file_name):
    # Ensure output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Render the template with context
    context = {
        "table_data": table_data,
        "page_number": 1,  # Example static page number; modify as needed
    }
    html_content = render_to_string("table_pdf_template.html", context)

    # Generate PDF from HTML
    output_path = os.path.join(output_directory, file_name)
    # HTML(string=html_content).write_pdf(output_path)

    return output_path


@csrf_exempt
def generate_pdf_duplicate(request):
    try:
        if request.method == "POST":
            data = json.loads(request.body)
            utc_time = datetime.utcnow()

            request_header = request.headers
            auth_token = request_header["Authorization"]
            access_token = data["access_token"]

            state, msg, user = authorization(auth_token=auth_token, access_token=access_token)
            if not state:
                return JsonResponse(msg, safe=False)

            table_data = data.get("table_data")
            if not table_data:
                return JsonResponse({
                    "status": 400,
                    "action_status": "error",
                    "message": "Missing table details"
                }, safe=False)

            data_uniq_id = str(uuid.uuid4())
            output_directory = 'media/pdf/'
            file_name = f"{data_uniq_id}.pdf"

            result = generate_pdf_with_template(table_data, output_directory, file_name)

            if result:
                return JsonResponse({
                    "status": 200,
                    "action_status": "success",
                    "message": "PDF generated successfully",
                    "pdf_path": result
                }, safe=False)
            else:
                return JsonResponse({
                    "status": 400,
                    "action_status": "error",
                    "message": "Failed to generate PDF"
                }, safe=False)
        else:
            return JsonResponse({
                "status": 400,
                "action_status": "error",
                "message": "Invalid request method"
            }, safe=False)
    except Exception as Err:
        response_exception(Err)
        return JsonResponse(str(Err), safe=False)
