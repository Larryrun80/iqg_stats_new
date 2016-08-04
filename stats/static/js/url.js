function ReplaceUrl(url,key,value)
{
    var fragPos = url.lastIndexOf("#");
    var fragment="";
    if(fragPos > -1)
    {
        fragment = url.substring(fragPos);
        url = url.substring(0,fragPos);
    }
    var querystart = url.indexOf("?");
    if(querystart < 0 )
    {
        url +="?"+key+"="+value;
    }
    else if (querystart==url.length-1)
    {
        url +=key+"="+value;
    }
    else
    {
        var Re = new RegExp(key+"=[^\\s&#]*","gi");
        if (Re.test(url))
            url=url.replace(Re,key+"="+value);
        else
            url += "&"+key+"="+value;
    }
    return url+fragment;
}