from dateutil.parser import parse as date_parser
from dateutil import rrule

orderKey = 'ORDER'
visitKey = 'SITE_VISIT'
customerKey = 'CUSTOMER'
dateKey = 'event_time'
totalAmountKey = 'total_amount'


def countNoOfWeeks(startDate, endDate):
    weeksCount = rrule.rrule(rrule.WEEKLY, dtstart=startDate, until=endDate).count();
    return weeksCount

def fileToData(file_path, events):
    isFirstTime = True
    with open(file_path) as file:
        for line in file.readlines():
            if not isFirstTime:
                lineEvent = line.strip()[:-1]
            else:
                isFirstTime = False
                lineEvent = line.strip()[1:-1]
            ingest(lineEvent, events)

def writeOutput(fileName, data):
    with open(fileName, 'w') as f:
        for tuple in data:
            f.write(tuple[0] + ', ' + str(tuple[1]) + '\n')

def ingest(e, D):
    dictionary = eval(e)
    if dateKey in dictionary:
        dictionary[dateKey] = date_parser(dictionary[dateKey])

    custId = dictionary['customer_id'] if dictionary['type'] != customerKey \
                  else dictionary['key']

    if custId not in D:
        # Add new customer id
        D[custId] = [dictionary]
    else:
        # Add data of the customer id
        D[custId].append(dictionary)

def topXSimpleLTVCustomers(x, D):
    LTVArray = []
    for custId in D:

        # Visits for each week
        vkey = visitKey if visitKey in [tuple['type'] for tuple in D[custId]] else 'ORDER'
        listOfVisitDates = [tuple[dateKey] for tuple in D[custId] if tuple['type'] == vkey]
        if listOfVisitDates and 'ORDER' in [tuple['type'] for tuple in D[custId]]:
            noOfActiveWeeks = countNoOfWeeks(min(listOfVisitDates), max(listOfVisitDates))
            noOfVisits = float(len(listOfVisitDates))
            perWeekVisits = noOfVisits / noOfActiveWeeks

            dataForOrder = [ (tuple['key'], tuple['verb'], tuple['event_time'], float(tuple[totalAmountKey].split()[0]))
                           for tuple in D[custId] if tuple['type'] == orderKey ]
            orderAmountsByCustId = {}

            for key, verb, eventDate, amount in dataForOrder:
                if key not in orderAmountsByCustId:
                    orderAmountsByCustId[key] = (eventDate, amount)
                else:
                    if eventDate > orderAmountsByCustId[key][0]:
                        # Replace amount if newer update exists
                        orderAmountsByCustId[key] = (eventDate, amount)
            totalAmounts = sum([orderAmountsByCustId[k][1] for k in orderAmountsByCustId])
            totalExpenditureEveryVisit = float(totalAmounts) / perWeekVisits

            # LTV
            averageCustValuePerWeek = totalExpenditureEveryVisit * perWeekVisits
            lifespanOfCustomer = 10
            LTVArray.append( (custId, 52 * averageCustValuePerWeek * lifespanOfCustomer) )
        else:
            # No of Events for ORDER
            LTVArray.append( (custId, 0) )

    LTVArray.sort(reverse=True, key=lambda y: y[1])
    return LTVArray[:x]


if __name__ == '__main__':
    customer_info = {}
    fileToData("../input/inputfile.txt", customer_info)
    top_LTVs = topXSimpleLTVCustomers(10, customer_info)
    output_file = "../output/outputfile.txt"
    writeOutput(output_file, top_LTVs)
    print ("\nData saved in: {}".format(output_file))