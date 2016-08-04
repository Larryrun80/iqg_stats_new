function CheckDate(str) {
    if(str.match(/^((?:19|20|21)\d\d)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$/)) { 
        return true; 
    } else {
        return false;
    }
}