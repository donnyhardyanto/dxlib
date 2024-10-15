"use strict";Object.defineProperty(exports, "__esModule", {value: true});var _chunkAF3BFCPYcjs = require('./chunk-AF3BFCPY.cjs');var _chunk73UZBSA6cjs = require('./chunk-73UZBSA6.cjs');var t=["ca","com","biz","info","name","net","org"];var i=["gmail.com","yahoo.ca","hotmail.com"];var N={domain_suffix:t,free_email:i},n=N;var l=["Argentia","Asbestos","Baddeck","Baie-Comeau","Bancroft","Banff","Barkerville","Barrie","Bathurst","Batoche","Belleville","Beloeil","Bonavista","Borden","Brampton","Brandon","Brantford","Brockville","Brooks","Burlington","Burnaby","Calgary","Cambridge","Campbell River","Cap-de-la-Madeleine","Caraquet","Cavendish","Chambly","Channel-Port aux Basques","Charlesbourg","Charlottetown","Ch\xE2teauguay","Chatham","Chatham-Kent","Chibougamau","Chilliwack","Churchill","Corner Brook","Cornwall","C\xF4te-Saint-Luc","Courtenay","Cranbrook","Cumberland House","Dalhousie","Dauphin","Dawson","Dawson Creek","Delta","Digby","Dorval","Edmonton","Elliot Lake","Esquimalt","Estevan","Etobicoke","Ferryland","Flin Flon","Fort Erie","Fort Frances","Fort McMurray","Fort Saint James","Fort Saint John","Fort Smith","Fredericton","Gananoque","Gander","Gasp\xE9","Gatineau","Glace Bay","Granby","Grand Falls\u2013Windsor","Grande Prairie","Guelph","Halifax","Hamilton","Happy Valley\u2013Goose Bay","Harbour Grace","Havre-Saint-Pierre","Hay River","Hope","Hull","Inuvik","Iqaluit","Iroquois Falls","Jasper","Jonqui\xE8re","Kamloops","Kapuskasing","Kawartha Lakes","Kelowna","Kenora","Kildonan","Kimberley","Kingston","Kirkland Lake","Kitchener","Kitimat","Kuujjuaq","La Salle","La Tuque","Labrador City","Lachine","Lake Louise","Langley","Laurentian Hills","Laval","Lethbridge","L\xE9vis","Liverpool","London","Longueuil","Louisbourg","Lunenburg","Magog","Matane","Medicine Hat","Midland","Miramichi","Mississauga","Moncton","Montreal","Montr\xE9al-Nord","Moose Factory","Moose Jaw","Moosonee","Nanaimo","Nelson","New Westminster","Niagara Falls","Niagara-on-the-Lake","North Bay","North Vancouver","North York","Oak Bay","Oakville","Orillia","Oshawa","Ottawa","Parry Sound","Penticton","Perc\xE9","Perth","Peterborough","Picton","Pictou","Placentia","Port Colborne","Port Hawkesbury","Port-Cartier","Powell River","Prince Albert","Prince George","Prince Rupert","Quebec","Quesnel","Red Deer","Regina","Revelstoke","Rimouski","Rossland","Rouyn-Noranda","Saguenay","Saint Albert","Saint Anthony","Saint Boniface","Saint Catharines","Saint John","Saint Thomas","Saint-Eustache","Saint-Hubert","Sainte-Anne-de-Beaupr\xE9","Sainte-Foy","Sainte-Th\xE9r\xE8se","Sarnia-Clearwater","Saskatoon","Sault Sainte Marie","Scarborough","Sept-\xCEles","Sherbrooke","Simcoe","Sorel-Tracy","Souris","Springhill","St. John\u2019s","Stratford","Sudbury","Summerside","Swan River","Sydney","Temiskaming Shores","Thompson","Thorold","Thunder Bay","Timmins","Toronto","Trail","Trenton","Trois-Rivi\xE8res","Tuktoyaktuk","Uranium City","Val-d\u2019Or","Vancouver","Vernon","Victoria","Wabana","Waskaganish","Waterloo","Watson Lake","Welland","West Nipissing","West Vancouver","White Rock","Whitehorse","Windsor","Winnipeg","Woodstock","Yarmouth","Yellowknife","York","York Factory"];var s=["{{location.city_prefix}} {{person.firstName}}{{location.city_suffix}}","{{location.city_prefix}} {{person.firstName}}","{{person.firstName}}{{location.city_suffix}}","{{person.last_name.generic}}{{location.city_suffix}}","{{location.city_name}}"];var m=["A#? #?#","B#? #?#","C#? #?#","E#? #?#","G#? #?#","H#? #?#","J#? #?#","K#? #?#","L#? #?#","M#? #?#","N#? #?#","P#? #?#","R#? #?#","S#? #?#","T#? #?#","V#? #?#","X#? #?#","Y#? #?#"];var e="[0-9][ABCEGHJ-NPRSTVW-Z] [0-9][ABCEGHJ-NPRSTVW-Z][0-9]",p={AB:`{{helpers.fromRegExp(T${e})}}`,BC:`{{helpers.fromRegExp(V${e})}}`,MB:`{{helpers.fromRegExp(R${e})}}`,NB:`{{helpers.fromRegExp(E${e})}}`,NL:`{{helpers.fromRegExp(A${e})}}`,NS:`{{helpers.fromRegExp(B${e})}}`,NT:`{{helpers.fromRegExp(X${e})}}`,NU:`{{helpers.fromRegExp(X${e})}}`,ON:`{{helpers.fromRegExp([KLMNP]${e})}}`,PE:`{{helpers.fromRegExp(C${e})}}`,QC:`{{helpers.fromRegExp([GHJ]${e})}}`,SK:`{{helpers.fromRegExp(S${e})}}`,YT:`{{helpers.fromRegExp(Y${e})}}`};var u=["Alberta","British Columbia","Manitoba","New Brunswick","Newfoundland and Labrador","Nova Scotia","Northwest Territories","Nunavut","Ontario","Prince Edward Island","Quebec","Saskatchewan","Yukon"];var f=["AB","BC","MB","NB","NL","NS","NU","NT","ON","PE","QC","SK","YT"];var c=["{{person.firstName}} {{location.street_suffix}}","{{person.lastName}} {{location.street_suffix}}"];var P={city_name:l,city_pattern:s,postcode:m,postcode_by_state:p,state:u,state_abbr:f,street_pattern:c},d=P;var L={title:"English (Canada)",code:"en_CA",country:"CA",language:"en",endonym:"English (Canada)",dir:"ltr",script:"Latn"},h=L;var x={generic:[{value:"{{person.last_name.generic}}",weight:95},{value:"{{person.last_name.generic}}-{{person.last_name.generic}}",weight:5}]};var R={last_name_pattern:x},g=R;var y=["!##-!##-####","(!##)!##-####","!##.!##.####","1-!##-###-####","!##-!##-#### x###","(!##)!##-#### x###","1-!##-!##-#### x###","!##.!##.#### x###","!##-!##-#### x####","(!##)!##-#### x####","1-!##-!##-#### x####","!##.!##.#### x####","!##-!##-#### x#####","(!##)!##-#### x#####","1-!##-!##-#### x#####","!##.!##.#### x#####"];var C=["+1!##!######","+1!#########"];var S=["(!##) !##-####","(!##) ###-####"];var E={human:y,international:C,national:S},b=E;var _={format:b},B=_;var v={internet:n,location:d,metadata:h,person:g,phone_number:B},k= exports.a =v;var Se=new (0, _chunk73UZBSA6cjs.m)({locale:[k,_chunkAF3BFCPYcjs.a,_chunk73UZBSA6cjs.n]});exports.a = k; exports.b = Se;
