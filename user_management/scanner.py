# # import win32com.client
# import os
# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# # import pythoncom
# from datetime import datetime
# from django.conf import settings
# import requests

# @csrf_exempt
# def scan_document(request):
#     try:
#         pythoncom.CoInitialize()
#         wia = win32com.client.Dispatch("WIA.CommonDialog")
#         device = wia.ShowSelectDevice(1, True)
#         if not device:
#             message = {                        
#                 'action': 'error',
#                 'message': "No Device Connected"
#             }
#             return JsonResponse(message, safe=False, status=200)  

#         item = device.Items[0]
#         image = item.Transfer()
#         media_folder = 'media/scanning_image/'
#         os.makedirs(media_folder, exist_ok=True) 
#         current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
#         new_file_name = f"scanned_document_{current_datetime}.jpg"
#         file_path = os.path.join(media_folder, new_file_name)
#         image.SaveFile(file_path)
#         message = {                        
#             'action': 'success',
#             'data': file_path
#         }
#         return JsonResponse(message, safe=False, status=200)  

#     except Exception as e:
#         message = {                        
#             'action': 'error',
#             'message': str(e)
#         }
#         return JsonResponse(message, safe=False, status=200)

# @csrf_exempt
# def scan_document(request):
#     try:

#         url = "http://192.168.0.115:8001/scan_document"
#         response = requests.get(url)

#         data = response.content  # Corrected

#         # Define file path
#         filename = f"scanned_doc_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
#         save_path = os.path.join(settings.MEDIA_ROOT, "scanned_docs", filename)
#         os.makedirs(os.path.dirname(save_path), exist_ok=True)

#         # Write image to file
#         with open(save_path, "wb") as f:
#             f.write(data)

#         return JsonResponse({
#             "status": True,
#             "message": "Image saved successfully.",
#             "file_name": filename,
#             "file_path": f"{settings.MEDIA_URL}scanned_docs/{filename}"
#         })

#     except Exception as e:
#         return JsonResponse({"status": False, "message": str(e)}, status=500)
    

from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import os
from django.core.files.storage import default_storage
from django.utils import timezone

@csrf_exempt
def scan_document(request):
    try:
        if request.method == "POST" and request.FILES.get("image"):
            image = request.FILES["image"]
            timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
            filename = f"scan_{timestamp}.jpg"
            path = os.path.join("scans", filename)  # inside MEDIA_ROOT/scans

            saved_path = default_storage.save(path, image)

            return JsonResponse({
                "message": "Scan uploaded successfully.",
                "file_name": filename,
                "file_url": default_storage.url(saved_path)
            })

        return JsonResponse({"error": "No image uploaded"}, status=400)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
