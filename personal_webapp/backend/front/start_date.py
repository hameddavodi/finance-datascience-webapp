from django.http import HttpResponse

def start_date_endpoint(request):
    if request.method == 'GET':
        start_date = f'"{str(request.GET.get("start_date", ""))}"'
        
        # Store values in a text file
        with open('data.txt', 'w') as file:
            file.write(start_date)

        return HttpResponse('Value stored successfully in data.txt')
    else:
        return HttpResponse('Invalid request method', status=400)


     