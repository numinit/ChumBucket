/***************************************/
/************SORT FUNCTIONS*************/
/***************************************/
var sortType = 0;
var lastSortType = 0;
function sortTable(sortField) {
	//sort function for a table with a passed in table header object.
	//first find the column number
	var columnIndex = sortField.cellIndex;
	//first get all the student data over that field for each row and add it to an array of (data, tableRow)
	var dataArray = [];
	var rowArray = [];
	//dataArray.push("apple");
	var table = sortField.parentElement.parentElement;
	for (var i = 1; i < table.rows.length; i++) {
		//iterate through rows starting after the table header row
		var data = table.rows[i].cells[columnIndex].innerHTML;
		dataArray.push([i,data]);
		rowArray.push(table.rows[i]);
	}
	//then sort the array
	dataArray.sort(sortFunction);
	//reset the table rows
	//loop over the nodes and change the innerHTML of the table cells

	//first remove all the children rows
	for (var i = table.rows.length - 1; i > 0; i--) {
		table.deleteRow(i);
	}
	for (var i = 0; i < rowArray.length; i++) {
		var index = dataArray[i][0];
		var tNode = rowArray[index-1];
		table.appendChild(tNode);
	}
}

function sortFunction(a, b) {
	if (sortType == 1) {
		//number compare
		return a[1] - b[1];
	}
	else if (sortType == -1) {
		//reverse number compare
		return b[1] - a[1];
	}
	else if (sortType == 2) {
		//string compare
		return a[1].toString().localeCompare(b[1].toString());
	}
	else if (sortType == -2) {
		//reverse string compare
		return b[1].toString().localeCompare(a[1].toString());
	}
}

/***************************************/
/*******FORM VALIDATION FUNCTIONS*******/
/***************************************/
function validateField(data, errorField, errorText, type, required) {
	var retVal = true;
	var regex = /^(.*?)$/;
	//type == 0 for a name
	if (type == "username") {
		//username for 30 characters tops (only unique in district... usually uses initials and a number count)
		regex = /^[-\w\.\$@\*\!]{3,30}$/;
	}
	else if (type == "name") {
		regex = /^[A-Za-z0-9_ ]+$/;
	}
	else if (type == "password") {
		//password
		//regex = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{8,100}$/;
		regex = /^.{8,}$/;
	}
	else if (type == "password2") {
		//password
		//regex = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{8,100}$/;
		regex = /^.{8,}$/;
	}
	else if (type == "passwordTry") {
		//password attempt
		regex = /^(.*?)$/;
	}
	else if (type == "email") {
		//email
		regex = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$/;
	}
	else if (type == "emailTry") {
		//email attempt
		regex = /^(.*?)$/;
	}
	else if (type == "text") {
		//regular text
		regex = /^(.*?)$/;
	}
	else if (type == "multipleNotesWithProgress") {
		//note text
		regex = /^(.*?)$/;
	}
	else if (type == "select") {
		//select option so we don't care what it is
		regex = /^(.*?)$/;
	}
	else if (type == "radio") {
		//radio option so we don't care what it is
		regex = /^(.*?)$/;
	}
	else if (type == "image") {
		//file option so we don't care what it is
		regex = /^(.*?)$/;
	}
	else if (type == "png") {
		//file option so we don't care what it is
		regex = /^(.*?)$/;
	}
	else if (type == "file") {
		//file option so we don't care what it is
		regex = /^(.*?)$/;
	}
	else if (type == "date") {
		//date in the format mm-dd-yy like 2015-05-28
		regex = /^\d{4}-\d{2}-\d{2}$/;
	}
	else if (type == "dollar") {
		//dollar amount like -100.29 or 57
		regex = /^-?\d+(?:\.\d{0,2})?$/;
	}
	else if (type == "versionNumber") {
		//version number like 1.0.1
		regex = /^[0-9]+([.][0-9]+)*$/;
	}
	if (data == null || data == "") {
		if (required == "false") {
			errorField.innerHTML = "";
			return true;
		}
		errorField.innerHTML = "Must be filled out!";
		retVal = false;
	}
	else if (!regex.test(data)) {
		errorField.innerHTML = errorText;
		retVal = false;
	}
	else {
		errorField.innerHTML = "";
	}
	return retVal;
}

var notesCell1Text = "hey";
var notesCell2Text = "yo";
function createForm(formName, formDiv, formAction, formElements, submitText, cancelText, cancelLink) {
	var str = "";
	//see if this is a retry, if so then display a failed message here
	var rtVal = getUrlVars()["retry"];
	if (rtVal != undefined) {
		var rtMessage = getUrlVars()["retryMessage"];
		if (rtMessage != undefined) {
			str += "<div class='formRetryDiv'>" + rtMessage + "</div>";
		} else {
			str += "<div class='formRetryDiv'>" + submitText + " failed. Please check your fields and try again.</div>";
		}
	}
	//first create the form
	str += '<form id="' + formName + '" name="' + formName + '" method="POST" enctype="multipart/form-data" action="' + formAction + '" onsubmit="return checkForm(this);">';
	//create the apiKey
	str += '<input type="hidden" name="apiKey" value="3e9e924eded94831b1ee2f78b4ae95c3" />';
	//now create the table
	str += '<table class="formTable">';
	//now create the elements
	var rowIndex = -1;
	for (var i = 0; i < formElements.length; i++) {
		var name2 = formElements[i].name.split(' ').join('_');
		if (formElements[i].type == "hidden") {
			str += '<input type="hidden" name="' + name2 + '" val-type="' + formElements[i].type + '" value="' + formElements[i].value + '" />';
			continue;
		}
		rowIndex = rowIndex + 1;
		str += '<tr>';
		str += '<td class="formTableName" title="' + formElements[i].description + '">' + formElements[i].name + '</td>';
		str += '<td class="formTableInput">';
		switch (formElements[i].type) {
			case "name":
			case "text":
				str += '<input type="text" name="' + name2 + '" class="formTextInput" placeholder="' + formElements[i].description + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '"';
				var givenVal = "";
				if (typeof formElements[i].value !== undefined) {
					if (formElements[i].value.length > 0) {
						givenVal = formElements[i].value;
					}
				}
				var getVal = getUrlVars()[name2];
				if (getVal != undefined) {
					givenVal = getVal;
				}
				str += ' value="' + givenVal + '"';
				str += ' />';
				break;
			case "password":
			case "password2":
			case "passwordTry":
				str += '<input type="password" name="' + name2 + '" class="formTextInput" placeholder="' + formElements[i].description + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '"';
				var givenVal = "";
				if (typeof formElements[i].value !== undefined) {
					if (formElements[i].value.length > 0) {
						givenVal = formElements[i].value;
					}
				}
				var getVal = getUrlVars()[name2];
				if (getVal != undefined) {
					givenVal = getVal;
				}
				str += ' value="' + givenVal + '"';
				str += ' />';
				break;
			case "select":
				str += '<div class="formSelectInput">';
				str += '<select name="' + name2 + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '">';
				//get the provided value
				var givenValue = formElements[i].options[0].value;
				if (typeof formElements[i].value !== undefined) {
					if (formElements[i].value.length > 0) {
						givenVal = formElements[i].value;
					}
				}
				//check if the get variable is set for a retry
				var getVal = getUrlVars()[name2];
				if (getVal != undefined) {
					givenVal = getVal;
				}
				//now create the options
				for (var j = 0; j < formElements[i].options.length; j++) {
					if (formElements[i].options[j].value == givenVal) {
						str += '<option value="' + formElements[i].options[j].value + '" selected="selected">' + formElements[i].options[j].name + '</option>';
					} else {
						str += '<option value="' + formElements[i].options[j].value + '">' + formElements[i].options[j].name + '</option>';
					}
				}
				str += '</select>';
				str += '</div>';
				break;
			case "png":
				str += '<input type="file" id="' + name2 + 'FileID" name="' + formElements[i].name + '" class="formFileInput" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '" accept="image/png" data-multiple-caption="{count} files selected" onchange="inputFileChanged(this, ' + "'Choose File'" + ');"';
				if (formElements[i].multiple == "true") {
					str += ' multiple ';
				}
				str += '/>';
				str += '<label for="' + name2 + 'FileID">Choose File</label>';
				break;
			case "image":
				str += '<input type="file" id="' + name2 + 'FileID" name="' + formElements[i].name + '" class="formFileInput" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '" accept="image/png, image/jpeg" data-multiple-caption="{count} files selected" onchange="inputFileChanged(this, ' + "'Choose File'" + ');"';
				if (formElements[i].multiple == "true") {
					str += ' multiple ';
				}
				str += '/>';
				str += '<label for="' + name2 + 'FileID">Choose File</label>';
				break;
			case "file":
				str += '<input type="file" id="' + name2 + 'FileID" name="' + formElements[i].name + '" class="formFileInput" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '" data-multiple-caption="{count} files selected" onchange="inputFileChanged(this, ' + "'Choose File'" + ');"';
				if (formElements[i].multiple == "true") {
					str += ' multiple ';
				}
				str += '/>';
				str += '<label for="' + name2 + 'FileID">Choose File</label>';
				break;
			case "multipleNotesWithProgress":
				var exIndex = 0;
				var max = (formElements[i].existing != undefined && formElements[i].existing.length > 0) ? formElements[i].existing.length : 1;
				while (exIndex < max) {
					var noteValue = (formElements[i].existing != undefined && formElements[i].existing.length > 0) ? formElements[i].existing[exIndex]['note'] : "";
					var progressValue = (formElements[i].existing != undefined && formElements[i].existing.length > 0) ? formElements[i].existing[exIndex]['progress'] : "";

					if (exIndex > 0) {
						str += '<tr>';
						str += '<td class="formTableName" title="' + formElements[i].description + '">' + formElements[i].name + '</td>';
						str += '<td class="formTableInput">';
					}

					creationText = "";
					notesCell1Text = formElements[i].name;
					//we need to create an input for the note
					str += '<input type="text" name="' + name2 + '[]" class="formTextInput" placeholder="' + formElements[i].description + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '"';
					str += ' value="' + noteValue + '"';
					str += ' />';
					creationText += '<input type="text" name="' + name2 + '[]" class="formTextInput" placeholder="' + formElements[i].description + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '"';
					creationText += ' value=""';
					creationText += ' />';


					//we need to create a select for the note progress
					str += '<div class="formSelectInput">';
					str += '<select name="' + name2 + 'Progress[]" val-type="' + formElements[i].type + 'Progress" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '">';
					creationText += '<div class="formSelectInput">';
					creationText += '<select name="' + name2 + 'Progress[]" val-type="' + formElements[i].type + 'Progress" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '">';
					//get the provided value
					var givenValue = formElements[i].options[0].value;
					if (typeof formElements[i].value !== undefined) {
						if (formElements[i].value.length > 0) {
							givenVal = formElements[i].value;
						}
					}
					var givenVal2 = givenValue;
					if (progressValue.length > 0) {
						givenVal = progressValue;
					}
					//now create the options
					for (var j = 0; j < formElements[i].options.length; j++) {
						if (formElements[i].options[j].value == givenVal) {
							str += '<option value="' + formElements[i].options[j].value + '" selected="selected">' + formElements[i].options[j].name + '</option>';
						} else {
							str += '<option value="' + formElements[i].options[j].value + '">' + formElements[i].options[j].name + '</option>';
						}
						if (formElements[i].options[j].value == givenVal2) {
							creationText += '<option value="' + formElements[i].options[j].value + '" selected="selected">' + formElements[i].options[j].name + '</option>';
						} else {
							creationText += '<option value="' + formElements[i].options[j].value + '">' + formElements[i].options[j].name + '</option>';
						}
					}
					str += '</select>';
					str += '</div>';
					creationText += '</select>';
					creationText += '</div>';
					notesCell2Text = creationText;

					rowIndex = rowIndex + 1;
					str += '</td><td class="formTableError"></td>';
					str += '</tr>';

					exIndex += 1;
				}
				//then we need to create a button that will add another row to this table
				//with an input and a select box
				str += '<tr>';
				str += '<td class="formTableName">Add Note</td>';
				str += '<td><button type="button" class="flatButtonUnfilled" onclick="addNote(this);" data-ri="' + rowIndex + '">Add Note</button></td>';
				str += '<td class="formTableError"></td>';
				str += '</tr>';
				continue;
				break;
			default:
				str += '<input type="text" name="' + name2 + '" class="formTextInput" placeholder="' + formElements[i].description + '" val-type="' + formElements[i].type + '" val-required="' + formElements[i].required + '" val-error="' + formElements[i].error + '"';
				var givenVal = "";
				if (typeof formElements[i].value !== undefined) {
					if (formElements[i].value.length > 0) {
						givenVal = formElements[i].value;
					}
				}
				var getVal = getUrlVars()[name2];
				if (getVal != undefined) {
					givenVal = getVal;
				}
				str += ' value="' + givenVal + '"';
				str += ' />';
				break;
		}
		str += '</td><td class="formTableError" id="' + name2 + 'Error"></td>';
		str += '</tr>';
	}
	//now finish the tags
	str += '</table>';
	if (typeof submitText !== 'undefined') {
		str += '<button type="submit" class="flatButtonUnfilled">' + submitText + '</button>';
	}
	if (typeof cancelText !== 'undefined') {
		str += ' <a href="' + cancelLink + '">';
		str += '<button type="button" class="flatButtonUnfilled">' + cancelText + '</button>';
		str += '</a>';
	}
	str += '</form>';
	formDiv.innerHTML = str;
}
function addNote(src) {
	var rowIndex = src.getAttribute("data-ri");
	console.log(rowIndex);
	src.setAttribute("data-ri", parseInt(rowIndex) + 1);
	var table = src.parentNode.parentNode.parentNode;
	var row = table.insertRow(rowIndex);
	var cell1 = row.insertCell(0);
	var cell2 = row.insertCell(1);
	var cell3 = row.insertCell(2);

	cell1.className = "formTableName";
	cell2.className = "formTableInput";
	cell3.className = "formTableError";

	cell1.innerHTML = notesCell1Text;
	cell2.innerHTML = notesCell2Text;
}
function inputFileChanged(src, zeroText) {
	var label = src.nextElementSibling;
	var labelVal = label.innerHTML;
	var fileName = '';
	if (src.files && src.files.length > 1 ) {
		fileName = ( src.getAttribute( 'data-multiple-caption' ) || '' ).replace( '{count}', src.files.length );
	}
	else {
		fileName = src.value.split( '\\' ).pop();
	}
	if (fileName) {
		label.innerHTML = fileName;
	}
	else {
		label.innerHTML = zeroText;
	}
}

function checkForm(src) {
	var retVal = true;
	var p1 = null;
	var p2 = null;
	for (var i = 0; i < src.elements.length; i++) {
		var input = src.elements[i];
		if (input.name == "apiKey") {
			continue;
		} else if (input.name.length > 0) {
			//valid input
			var type = input.getAttribute("val-type");
			if (type == "hidden") {
				continue;
			}
			var req = input.getAttribute("val-required");
			var errorText = input.getAttribute("val-error");
			var errorElement = document.getElementById(input.name + "Error");
			if (errorElement == undefined) {
				continue;
			}
			//now we should check the input for validity
			if (!validateField(input.value, errorElement, errorText, type, req)) {
				retVal = false;
			}
			if (type == "password") {
				p1 = input;
			} else if (type == "password2") {
				p2 = input;
			}
		}
	}
	if (p1 !== null && p2 !== null) {
		if (p1.value != p2.value) {
			var e2 = document.getElementById(p2.name + "Error");
			e2.innerHTML = "Passwords don't match";
			retVal = false;
		}
	}
	return retVal;
}

/***************************************/
/***********DYNAMIC FUNCTIONS***********/
/***************************************/
function dynamicallyUpdateScript(showElement, showType, hideElement, updateElement, updateText, script, postData) {
	var xmlhttp;
	if (window.XMLHttpRequest){// code for IE7+, Firefox, Chrome, Opera, Safari
		xmlhttp=new XMLHttpRequest();
	}
	else {// code for IE6, IE5
		xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
	}
	xmlhttp.onreadystatechange=function(){
		if (xmlhttp.readyState==4){
			//if there is an authentication error then we should go to the home page
			if (xmlhttp.status == 401) {
				//authentication error so log the user out
				//test if we are on localhost
				var hostname = window.location.href;
				if (hostname.indexOf("localhost") > -1) {
					//find index of bitsites/xxx/
					console.log(window.location);
					var parts = hostname.split("/");
					console.log(parts);
					//combine the first six splits
					var newLocation = "";
					for (var i = 0; i < 6; i = i + 1) {
						newLocation += parts[i] + "/";
					}
					newLocation += "Account.php?logout=yes";
					document.location = newLocation;
				} else {
					document.location = window.location.hostname + "/Account.php?logout=yes";
				}
			} else {
				if (updateElement != "") {
					if (updateText == "") {
						updateElement.innerHTML = xmlhttp.responseText;
					}
					else {
						updateElement.innerHTML = updateText;
					}
				}
				if (hideElement != "") {
					hideElement.style.display = "none";
				}
				if (showElement != "") {
					showElement.style.display = showType;
				}
			}
		}
	}
	xmlhttp.open("POST",script,true);
	xmlhttp.setRequestHeader("Content-type","application/x-www-form-urlencoded");
	xmlhttp.send(postData);
}

/**
 * Call this to asynchronously call an API, and then get callbacks when finished
 */
function dynamicallyGetScript(method, script, postData, func, errorFunc, isRefresh) {
	if (isRefresh == undefined) {
    isRefresh = false;
  }
	var xmlhttp;
	if (window.XMLHttpRequest){// code for IE7+, Firefox, Chrome, Opera, Safari
		xmlhttp=new XMLHttpRequest();
	}
	else {// code for IE6, IE5
		xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
	}
	xmlhttp.onreadystatechange=function(){
		if (xmlhttp.readyState==4){
			//console.log(xmlhttp.responseText);
			var firstResponse = JSON.parse(xmlhttp.responseText);
			//if there is an authentication error we should try to refresh the token
			//if that works then we need to call this script again, otherwise logout the user
			if ((xmlhttp.status >= 400 && xmlhttp.status <= 499) || "error" in firstResponse) {
				if (xmlhttp.status == 401 && isRefresh == false) {
					//authentication error, try to refresh the token
					var xmlhttpRefresh;
					if (window.XMLHttpRequest){// code for IE7+, Firefox, Chrome, Opera, Safari
						xmlhttpRefresh=new XMLHttpRequest();
					}
					else {// code for IE6, IE5
						xmlhttpRefresh=new ActiveXObject("Microsoft.XMLHTTP");
					}
					xmlhttpRefresh.onreadystatechange=function(){
						if (xmlhttpRefresh.readyState==4){
							var repo = JSON.parse(xmlhttpRefresh.responseText);
							//if there is an authentication error then we should go to the home page
							if ((xmlhttpRefresh.status >= 400 && xmlhttpRefresh.status <= 499) || "error" in repo) {
								console.log("REFRESH FAILED");
								console.log(xmlhttpRefresh.status);
								console.log(xmlhttpRefresh.responseText);
								//on error
								document.location = "/Login?logout=yes";
							} else {
								console.log("REFRESH SUCCESS");
								console.log(xmlhttpRefresh.status);
								console.log(xmlhttpRefresh.responseText);
								//now call the function again
								dynamicallyGetScript(method, script, postData, func, errorFunc, true);
							}
						}
					}
					xmlhttpRefresh.open("GET","/RefreshToken",true);
					xmlhttpRefresh.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
					xmlhttpRefresh.send();
				} else {
					// use this if you specifically want to know if b was passed
			    if (errorFunc === undefined) {
			      //just print out the error
						console.log(xmlhttp.status);
						console.log(xmlhttp.responseText);
			    } else {
						//call the error function
						errorFunc(xmlhttp.responeText);
					}
				}
			} else {
				if (func === undefined) {
					//just print out the response
					console.log(xmlhttp.status);
					console.log(xmlhttp.responseText);
				} else {
					//call the function
					func(xmlhttp.responseText);
				}
			}
		}
	}
	xmlhttp.open(method,script,true);
	xmlhttp.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
	xmlhttp.setRequestHeader("X-XSRF-TOKEN", getCookie("xsrf_token"));
	if (method == "POST" || method == "PUT" || method == "DELETE") {
		xmlhttp.send(postData);
	} else if (method == "GET") {
		xmlhttp.send();//the data must be set in the script for GET functions
	}
}

/***************************************/
/***********COOKIE FUNCTIONS************/
/***************************************/
function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires="+d.toUTCString();
    document.cookie = cname + "=" + cvalue + "; " + expires;
}
function getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i=0; i<ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') {
			c = c.substring(1);
		}
        if (c.indexOf(name) == 0) {
			return c.substring(name.length, c.length);
		}
    }
    return "";
}
function checkCookie(cname) {
    var cookie = getCookie(cname);
    if (cookie != "") {
        return true;
    }
	else {
        return false;
    }
}

/***************************************/
/*********DATA HELPER FUNCTIONS*********/
/***************************************/
function numberToPlace(number) {
	if (number % 10 == 1 && (number % 100) - 11 != 0) {
		return (number + "st");
	}
	else if (number % 10 == 2 && (number % 100) - 12 != 0) {
		return (number + "nd");
	}
	else if (number % 10 == 3 && (number % 100) - 13 != 0) {
		return (number + "rd");
	}
	else {
		return (number + "th");
	}
}

function autoGrow(element) {
	element.style.height = "60px";
	element.style.height = (element.scrollHeight)+"px";
}

//resizes the text until the minFontSize then starts removing text
//based on the character input
function resizeText(element, required, height, minFontSize, removeCharacter) {
	if (element.innerHTML.length >= required) {
		element.innerHTML = element.innerHTML.substring(0, element.innerHTML.length - required);
	} else {
		element.innerHTML = "";
		return;
	}
	while (element.scrollHeight > height && element.style.fontSize.substring(0,2) > minFontSize) {
		element.style.fontSize = (element.style.fontSize.substring(0,2) - 1) + "px";
		element.style.lineHeight = (element.style.lineHeight.substring(0,2) - 1) + "px";
	}
	while (element.scrollHeight > height && element.innerHTML.length > 0) {
		if (removeCharacter == "all") {
			element.innerHTML = element.innerHTML.substr(0, element.innerHTML.length - 1);
		} else {
			var n = element.innerHTML.lastIndexOf(removeCharacter);
			element.innerHTML = element.innerHTML.substring(0, n);
		}
	}
}

function scrollToTop(scrollDuration) {
	const scrollHeight = window.scrollY;
    const scrollStep = Math.PI / ( scrollDuration / 15 );
    const cosParameter = scrollHeight / 2;
	var scrollCount = 0;
    var scrollMargin = 0;
    var scrollInterval = setInterval( function() {
            if ( window.scrollY != 0 ) {
                scrollCount = scrollCount + 1;
                scrollMargin = cosParameter - cosParameter * Math.cos( scrollCount * scrollStep );
                window.scrollTo( 0, ( scrollHeight - scrollMargin ) );
            }
            else clearInterval(scrollInterval);
        }, 15 );
}

//Fades an element out
function fadeOut(element,val,timing) {
	if(isNaN(val)) {
		val = 10;
	}
  	if (val == 10) {
		element.style.opacity='1.0';
		//For IE
		element.style.filter='alpha(opacity=1.0)';
	} else {
		element.style.opacity='0.'+val;
  		//For IE
  		element.style.filter='alpha(opacity='+val+'0)';
	}
  	if(val>0){
    	val--;
    	setTimeout(function() {fadeOut(element,val,timing)}, timing);
  	} else {
		return;
	}
}

// Element to fade, Fade value[min value is 0]
function fadeIn(element,val,timing){
  	if (isNaN(val)) {
	  val = 0;
	}
	if (val == 10) {
		element.style.opacity='1.0';
		//For IE
		element.style.filter='alpha(opacity=1.0)';
	} else {
		element.style.opacity='0.'+val;
  		//For IE
  		element.style.filter='alpha(opacity='+val+'0)';
	}
  	if (val<10) {
    	val++;
		setTimeout(function() {fadeIn(element,val,timing)}, timing);
  	} else {
		return;
	}
}

function formatBytes(bytes,decimals) {
   if(bytes == 0) return '0 Byte';
   var k = 1024; // or 1024 for binary
   var dm = decimals + 1 || 3;
   var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
   var i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function padNumber(n) {
    return (n < 10) ? ("0" + n) : n;
}

function formatDate(dd) {
	var ds = "" + getMonthName(dd.getMonth()) + " " + dd.getDate() + ", " + dd.getFullYear() + " ";
	if (dd.getHours() * 60 + dd.getMinutes() >= 720) {
		ds += padNumber(dd.getHours() - 12) + ":" + padNumber(dd.getMinutes()) + " PM";
	} else {
		ds += padNumber(dd.getHours()) + ":" + padNumber(dd.getMinutes()) + " AM";
	}
	return ds;
}

function getMonthName(month) {
	var ar = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
	return ar[month];
}

function getUrlVars() {
	var vars = {};
	var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m,key,value) {
		//vars[key] = decodeURI(value);
		vars[key] = decodeURIComponent(value.replace(/\+/g, ' '));
	});
	//now replace the special url symbols with their actual characters

	return vars;
}

/**
* Sets the form values from the get values in the URL
*/
function setFormValuesFromUrl(form) {
	var elements = document.getElementById(form).elements;
	var getVals = getUrlVars();
	for (var i = 0; i < elements.length; i = i + 1) {
		var getVal = getVals[elements[i].name];
		if (getVal != undefined) {
			var type = elements[i].getAttribute("val-type");
			if (type == "date") {
				$('#' + elements[i].name + 'Date').datepicker('update', getVal);
			} else if (type == "text" || type == "email" || type == "name") {
				$('#' + form).find('input[name="' + elements[i].name + '"]').val(getVal);
			} else if (type == "select") {
				$('#' + elements[i].name + 'Select').selectpicker('val', getVal);
			} else {
				document.forms[form][elements[i].name].value = getVal;
			}
		}
	}
}

/**
* Validates the form before submission
*/
function validateForm(src) {

	var retVal = true;
	var p1 = null;
	var p2 = null;
	for (var i = 0; i < src.elements.length; i++) {
		var input = src.elements[i];
		if (input.name.length > 0) {
			//valid input
			var type = input.getAttribute("val-type");
			if (type == "hidden") {
				continue;
			}
			var req = input.hasAttribute("required");
			var errorText = input.getAttribute("val-error");
			var errorElement = document.getElementById(input.name + "Error");
			if (errorElement == undefined) {
				continue;
			}
			//now we should check the input for validity
			if (!validateField(input.value, errorElement, errorText, type, req)) {
				retVal = false;
			}
			if (type == "password") {
				p1 = input;
			} else if (type == "password2") {
				p2 = input;
			}
		}
	}
	if (p1 !== null && p2 !== null) {
		if (p1.value != p2.value) {
			var e2 = document.getElementById(p2.name + "Error");
			e2.innerHTML = "Passwords don't match";
			retVal = false;
		}
	}
	return retVal;
}

/**
* Hides and clears the error fields of a form
* Then, attaches an update function so that the fields are automatically checked
*/
function setUpForm(src) {
	setFormValuesFromUrl(src);
	src = document.getElementById(src);
	for (var i = 0; i < src.elements.length; i++) {
		var input = src.elements[i];
		if (input.name.length > 0) {
			var type = input.getAttribute("val-type");
			if (type == "hidden") {
				continue;
			}
			//clear error fields
			var errorElement = document.getElementById(input.name + "Error");
			if (errorElement == undefined) {
				continue;
			} else {
				errorElement.innerHTML = "";
			}
			//attach an update function
			createUpdateFunc(input);
		}
	}
}

/**
* Attaches an update function to the provided input
*/
function createUpdateFunc(input) {
	if (input.getAttribute('val-type') == "date" || input.getAttribute('val-type') == "select") {
		//don't do anything
	} else if (input.getAttribute('val-type') == "password2") {
		//just check if the passwords match
		var form = input.form;
		var p1 = 0;
		for (var i = 0; i < form.elements.length; i++) {
			var i2 = form.elements[i];
			if (i2.name.length > 0) {
				var type = i2.getAttribute("val-type");
				if (type == "password") {
					p1 = form.elements[i];
					break;
				}
			}
		}
		$(input).on('input', function() {
			var errorText2 = input.getAttribute("val-error");
			var errorElement2 = document.getElementById(input.name + "Error");
			if ($(input).val() != $(p1).val()) {
				errorElement2.innerHTML = "Passwords don't match";
			} else {
				errorElement2.innerHTML = "";
			}
		});
	} else {
		$(input).on('input', function() {
			var req2 = input.hasAttribute("required");
			var errorText2 = input.getAttribute("val-error");
			var errorElement2 = document.getElementById(input.name + "Error");
			var type2 = input.getAttribute("val-type");
			validateField($(this).val(), errorElement2, errorText2, type2, req2);
		});
	}
}
