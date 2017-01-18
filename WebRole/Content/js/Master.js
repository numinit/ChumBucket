//Scroll to show elements on the screen
//just add the class slideanim to an element to have it slide in on scroll
$(window).scroll(function() {
  $(".slideanim").each(function(){
    var pos = $(this).offset().top;

    var winTop = $(window).scrollTop();
    if (pos < winTop + 600) {
      $(this).addClass("slide");
    }
  });
});

//Event for pressing enter on an input
//example: $('textarea').pressEnter(function(){alert('here')})
$.fn.pressEnter = function(fn) {  
	return this.each(function() {  
  	$(this).bind('enterPress', fn);
    $(this).keyup(function(e){
    	if(e.keyCode == 13) {
    		$(this).trigger("enterPress");
    	}
    })
	});  
};

//Shake event for text or an input to show that an event has taken place
jQuery.fn.shake = function(intShakes, intDistance, intDuration) {
	this.each(function() {
		$(this).css("position","relative"); 
	  for (var x=1; x<=intShakes; x++) {
	  	$(this).animate({left:(intDistance*-1)}, (((intDuration/intShakes)/4)))
	    .animate({left:intDistance}, ((intDuration/intShakes)/2))
	    .animate({left:0}, (((intDuration/intShakes)/4)));
	  }
	});
	return this;
};