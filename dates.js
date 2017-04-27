'use strict';

var dateFormat = module.exports.dateFormat = function dateFormat(date, idFeed)
{
    switch(idFeed)
    {
        case 1:
            var dataBwin = date.split("/");
            var timeBwin = dataBwin[1].trim().split(':');
            var endTimeBwin = convertHour(timeBwin[0])+":"+timeBwin[1];
            var dateAuxBwin = dataBwin[0].replace(/\./g,"-");
            var dateAuxSpaceBwin = dateAuxBwin.replace(/ /g,"");
            var dateBwin = dateAuxSpaceBwin.split("-");
            return dateBwin[2]+"-"+dateBwin[1]+"-"+dateBwin[0]+" "+endTimeBwin.trim()+":00";
            break;
        case 2:
            return date.replace(/T/g," ").replace(/Z/g,":00");
        break;
        case 3:
            return date;
        break;
        case 4:
            return date.replace(/T/g," ");
        break;
        case 5:
            return date.replace(/T/g," ");
        break;
        case 6:
            return date.replace(/T|Z/g," ");
        break;
        case 7:
            return date.replace(/T/g," ").replace(/Z/g,":00");
        break;
        case 8:
            return date.replace(/T|Z/g," ");
        break;
        case 9:
            return date.replace(/T/g," ").replace(/Z/g,":00");
        break;
        case 10:
            return date.replace(/T|Z/g," ");
        break;
        case 11:
        break;
        case 12:
            return date.replace(/T/g," ").replace(/Z/g,":00");
        break;
        case 13:
            return date.replace(/T|Z/g," ");
        break;
        case 14:
        break;
        case 15:
            return date.replace(/T|Z/g," ");
        break;
        case 16:
            return date.replace(/T/g," ").replace(/Z/g,"");
        break;
        case 17:
            // 11/01/2017 21:30:00
            var strAux = date.split(" "),
                strDate = strAux[0].split("/"),
                strHour = strAux[1];
            // 2017-01-11 21:30:00    
            return strDate[2]+"-"+strDate[1]+"-"+strDate[0]+" "+strHour;
        break;
        default:
            return date;
        break;

    }
};