/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.maxmind;

import com.google.common.collect.Maps;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.model.InsightsResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import com.maxmind.geoip2.record.Traits;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class Schemaless {

  public static Map<String,Object> representedCountry(RepresentedCountry representedCountry) {
    if (null == representedCountry) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("name", representedCountry.getName());
    struct.put("type", representedCountry.getType());
    // struct.put("names", representedCountry.getNames());
    // struct.put("geoNameId", representedCountry.getGeoNameId());
    return struct;
  }

  public static Map<String,Object> continent(Continent continent) {
    if (null == continent) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("name", continent.getName());
    struct.put("code", continent.getCode());
    // struct.put("names", continent.getNames());
    // struct.put("geoNameId", continent.getGeoNameId());
    return struct;
  }

  public static Map<String,Object> location(Location location) {
    if (null == location) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("accuracyRadius", location.getAccuracyRadius());
    // struct.put("averageIncome", location.getAverageIncome());
    struct.put("latitude", location.getLatitude());
    struct.put("longitude", location.getLongitude());
    // struct.put("metroCode", location.getMetroCode());
    // struct.put("populationDensity", location.getPopulationDensity());
    struct.put("timeZone", location.getTimeZone());
    return struct;
  }

  public static Map<String,Object> postal(Postal postal) {
    if (null == postal) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("code", postal.getCode());
    // struct.put("confidence", postal.getConfidence());
    return struct;
  }

  public static Map<String,Object> city(City city) {
    if (null == city) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("name", city.getName());
    // struct.put("confidence", city.getConfidence());
    // struct.put("names", city.getNames());
    // struct.put("geoNameId", city.getGeoNameId());
    return struct;
  }

  public static Map<String,Object> country(Country country) {
    if (null == country) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("name", country.getName());
    // struct.put("confidence", country.getConfidence());
    struct.put("isoCode", country.getIsoCode());
    // struct.put("names", country.getNames());
    // struct.put("geoNameId", country.getGeoNameId());
    return struct;
  }

  public static Map<String,Object> subdivision(Subdivision subdivision) {
    if (null == subdivision) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("name", subdivision.getName());
    // struct.put("confidence", subdivision.getConfidence());
    struct.put("isoCode", subdivision.getIsoCode());
    // struct.put("names", subdivision.getNames());
    // struct.put("geoNameId", subdivision.getGeoNameId());
    return struct;
  }

  public static Map<String,Object> traits(Traits traits) {
    if (null == traits) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("autonomousSystemNumber", traits.getAutonomousSystemNumber());
    struct.put("autonomousSystemOrganization", traits.getAutonomousSystemOrganization());
    struct.put("connectionType", traits.getConnectionType());
    struct.put("domain", traits.getDomain());
    struct.put("isAnonymousProxy", traits.isAnonymousProxy());
    struct.put("isSatelliteProvider", traits.isSatelliteProvider());
    struct.put("isp", traits.getIsp());
    struct.put("organization", traits.getOrganization());
    struct.put("userType", traits.getUserType());
    struct.put("isLegitimateProxy", traits.isLegitimateProxy());
    return struct;
  }

  public static Map<String,Object>  countryResponse(CountryResponse countryResponse) {
    if (null == countryResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("continent", continent(countryResponse.getContinent()));
    struct.put("country", country(countryResponse.getCountry()));
    struct.put("registeredCountry", country(countryResponse.getRegisteredCountry()));
    // struct.put("representedCountry", representedCountry(countryResponse.getRepresentedCountry()));
    struct.put("traits", traits(countryResponse.getTraits()));
    return struct;
  }

  public static Map<String,Object>  domainResponse(DomainResponse domainResponse) {
    if (null == domainResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("domain", domainResponse.getDomain());
    return struct;
  }

  public static Map<String,Object>  connectionTypeResponse(ConnectionTypeResponse connectionTypeResponse) {
    if (null == connectionTypeResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("connectionType", connectionTypeResponse.getConnectionType());
    return struct;
  }

  public static Map<String,Object>  cityResponse(CityResponse cityResponse) {
    if (null == cityResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("city", city(cityResponse.getCity()));
    struct.put("location", location(cityResponse.getLocation()));
    struct.put("postal", postal(cityResponse.getPostal()));
    List<Map<String,Object>> subdivisionsList = new ArrayList();
    for (Subdivision s : cityResponse.getSubdivisions()) {
      Map<String, Object> subdivisionStruct = subdivision(s);
      subdivisionsList.add(subdivisionStruct);
    }
    struct.put("subdivisions", subdivisionsList);
    struct.put("continent", continent(cityResponse.getContinent()));
    struct.put("country", country(cityResponse.getCountry()));
    struct.put("registeredCountry", country(cityResponse.getRegisteredCountry()));
    // struct.put("representedCountry", representedCountry(cityResponse.getRepresentedCountry()));
    // struct.put("traits", traits(cityResponse.getTraits()));
    return struct;
  }

  public static Map<String,Object> asnResponse(AsnResponse asnResponse) {
    if (null == asnResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("autonomousSystemNumber", asnResponse.getAutonomousSystemNumber());
    struct.put("autonomousSystemOrganization", asnResponse.getAutonomousSystemOrganization());
    return struct;
  }

  public static Map<String,Object> insightsResponse(InsightsResponse insightsResponse) {
    if (null == insightsResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("city", city(insightsResponse.getCity()));
    struct.put("location", location(insightsResponse.getLocation()));
    struct.put("postal", postal(insightsResponse.getPostal()));
    List<Map<String,Object>> subdivisionsList = new ArrayList();
    for (Subdivision s : insightsResponse.getSubdivisions()) {
      Map<String, Object> subdivisionStruct = subdivision(s);
      subdivisionsList.add(subdivisionStruct);
    }
    struct.put("subdivisions", subdivisionsList);
    struct.put("continent", continent(insightsResponse.getContinent()));
    struct.put("country", country(insightsResponse.getCountry()));
    struct.put("registeredCountry", country(insightsResponse.getRegisteredCountry()));
    struct.put("representedCountry", representedCountry(insightsResponse.getRepresentedCountry()));
    struct.put("traits", traits(insightsResponse.getTraits()));
    return struct;
  }

  public static Map<String,Object> enterpriseResponse(EnterpriseResponse enterpriseResponse) {
    if (null == enterpriseResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("city", city(enterpriseResponse.getCity()));
    struct.put("location", location(enterpriseResponse.getLocation()));
    struct.put("postal", postal(enterpriseResponse.getPostal()));
    List<Map<String,Object>> subdivisionsList = new ArrayList();
    for (Subdivision s : enterpriseResponse.getSubdivisions()) {
      Map<String, Object> subdivisionStruct = subdivision(s);
      subdivisionsList.add(subdivisionStruct);
    }
    struct.put("subdivisions", subdivisionsList);
    struct.put("continent", continent(enterpriseResponse.getContinent()));
    struct.put("country", country(enterpriseResponse.getCountry()));
    struct.put("registeredCountry", country(enterpriseResponse.getRegisteredCountry()));
    struct.put("representedCountry", representedCountry(enterpriseResponse.getRepresentedCountry()));
    struct.put("traits", traits(enterpriseResponse.getTraits()));
    return struct;
  }

  public static Map<String,Object> anonymousIpResponse(AnonymousIpResponse anonymousIpResponse) {
    if (null == anonymousIpResponse) {
      return null;
    }
    Map<String,Object> struct = Maps.newHashMap();
    struct.put("isAnonymous", anonymousIpResponse.isAnonymous());
    struct.put("isAnonymousVpn", anonymousIpResponse.isAnonymousVpn());
    struct.put("isHostingProvider", anonymousIpResponse.isHostingProvider());
    struct.put("isPublicProxy", anonymousIpResponse.isPublicProxy());
    struct.put("isTorExitNode", anonymousIpResponse.isTorExitNode());
    return struct;
  }

  public static Map<String,Object> ispResponse(IspResponse ispResponse) {
    if (null == ispResponse) {
      return null;
    }

    Map<String,Object> struct = Maps.newHashMap();
    struct.put("isp", ispResponse.getIsp());
    struct.put("organization", ispResponse.getOrganization());
    return struct;
  }

  static Map<String,Object> struct(AnonymousIpResponse anonymousIpResponse, AsnResponse asnResponse, CityResponse cityResponse, ConnectionTypeResponse connectionTypeResponse, CountryResponse countryResponse, DomainResponse domainResponse, EnterpriseResponse enterpriseResponse, InsightsResponse insightsResponse, IspResponse ispResponse) {
        Map<String,Object> ret = Maps.newHashMap();
        if(null != anonymousIpResponse){
          ret.put("anonymousIpResponse", anonymousIpResponse(anonymousIpResponse));
        }
        if(null != asnResponse){
          ret.put("asnResponse", asnResponse(asnResponse));
        }
        if(null != cityResponse){
          ret.put("cityResponse", cityResponse(cityResponse));
        }
        if(null != connectionTypeResponse){
          ret.put("connectionTypeResponse", connectionTypeResponse(connectionTypeResponse));
        }
        if(null != countryResponse){
          ret.put("countryResponse", countryResponse(countryResponse));
        }
        if(null != domainResponse){
          ret.put("domainResponse", domainResponse(domainResponse));
        }
        if(null != enterpriseResponse){
          ret.put("enterpriseResponse", enterpriseResponse(enterpriseResponse));
        }
        if(null != insightsResponse){
          ret.put("insightsResponse", insightsResponse(insightsResponse));
        }
        if(null != ispResponse){
          ret.put("ispResponse", ispResponse(ispResponse));
        }
      return ret;
  }


}
