(function($) {
     var BOSH_SERVICE = '/bosh/http-bind',
         connection,
         _output,
         _log,
         _response,
         _input,
         _button;

     $(document).ready(function () {
         connection = $.strophe({
             url: BOSH_SERVICE,
             rawInput: raw_input,
             rawOutput: raw_output
         });

         _output = $('#output');
         _log = $('#log');
         _response = $('#response');

         $('#main ul.nav a').click(function(ev) {
             ev.preventDefault();
             show_output(this.hash);
         });

         _input = focus($('#input').submit(input_submit));
         _button = $('#connect');
     });

     // -------------------- UI --------------------

     function print(query, response, cls) {
         cls = cls || 'reply';
         $('<div class="entry"/>')
             .append($('<span class="query"/>').text(query))
             .append($('<div/>').addClass(cls).text(response))
             .appendTo(_response);
         show_output();
     }

     function print_error(query, message) {
         print(query, message, 'error');
     }

     function log(msg) {
         _log.append($('<div/>').text(msg));
         show_output();
     }

     function raw_input(data) {
         log('RECV: ' + data);
     }

     function raw_output(data) {
         log('SENT: ' + data);
     }

     function show_output(panel) {
         if (panel) {
             update_nav('#main ul.nav', panel);
             show_panel(panel);
         }
         // Scroll to the bottom.
         _output.attr('scrollTop', _output.attr('scrollHeight'));
     }

     function show_panel(panel) {
         return $(panel).removeClass('disabled')
             .siblings('.panel').addClass('disabled')
             .end();
     }

     function update_nav(nav, active) {
         $(nav).find('a').removeClass('active')
             .filter('[hash="' + active + '"]').addClass('active');
     }

     function input_submit(ev) {
         ev.preventDefault();
         if (is_connected()) {
             query(this.prompt.value);
         }
         else {
             connect(this.jid.value, this.pass.value);
         }
     }

     function connect(jid, pass) {
         _button.val('send');
         show_output('#log');
         connection.connect(jid, pass, connecting);
     }

     function connecting(status) {
         if (status == Strophe.Status.CONNECTING) {
             log('Strophe is connecting.');
         } else if (status == Strophe.Status.CONNFAIL) {
             log('Strophe failed to connect.');
             disconnected();
         } else if (status == Strophe.Status.DISCONNECTING) {
             log('Strophe is disconnecting.');
         } else if (status == Strophe.Status.DISCONNECTED) {
             log('Strophe is disconnected.');
             disconnected();
         } else if (status == Strophe.Status.CONNECTED) {
             log('Strophe is connected.');
             connected();
         }
     }

     function is_connected() {
         return _button.val() == 'send';
     }

     function connected() {
         focus(show_panel('#query'));
         show_output('#response');
     }

     function disconnected() {
         _button.val('connect');
         focus(show_panel('#login'));
     }

     function focus(elem) {
         return elem.find('input[type="text"]:first').focus().end();
     }

     function query(expr) {
         evaluate({
             query: expr,
             success: function(reply) { print(expr, reply); },
             error: function(message) { print_error(expr, message); }
         });
     }

     // -------------------- Queries --------------------

     function evaluate(opt) {
         send_iq({
             iq: query_iq(opt.query),
             success: function(iq) { query_response(iq, opt.success); },
             error: function(iq) { query_error(iq, opt.error); }
         });
     }

     function query_iq(query) {
         return $iq({ type: 'get' })
             .c('query', { xmlns: 'urn:message' })
             .t(Base64.encode(query));
     }

     function query_response(iq, k) {
         k(Base64.decode(iq.childNodes[0].textContent));
     }

     function query_error(iq, k) {
         k($(iq).find('text').text());
     }

     // -------------------- BOSH --------------------

     function send_iq(opt) {
         return connection.sendIQ(
             opt.iq,
             opt.success,
             opt.error || iq_error,
             opt.timeout || 2000
         );
     }

     function iq_error(data) {
         console.error('IQ failed!', data);
     }

     $.strophe = function(settings) {
         return $.extend(new Strophe.Connection(settings.url), settings);
     };

 })(jQuery);

