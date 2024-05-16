import datetime
import re


def datecal(urlreplace, datetype, ands, urldate):
    year = datetime.datetime.now().year

    if datetype == 'year':
        if ands == '+':
            urlyear = int(year) + int(urldate)
        elif ands == '-':
            urlyear = int(year) - int(urldate)
        urlreplace = re.sub('{(year|day)((\-|\+)(\d+))?}', str(urlyear), urlreplace)

    elif datetype == 'day':
        if ands == '+':
            urlyear = str(datetime.date.today() + datetime.timedelta(+int(urldate)))
        elif ands == '-':
            urlyear = str(datetime.date.today() + datetime.timedelta(-int(urldate)))

        urlreplace = re.sub('{(year|day)((\-|\+)(\d+))?}', str(urlyear), urlreplace)

    return (urlreplace)
