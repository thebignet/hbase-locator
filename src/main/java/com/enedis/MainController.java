package com.enedis;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
public class MainController {

    private LocalityService localityService;

    public MainController(LocalityService localityService) {
        this.localityService = localityService;
    }

    @RequestMapping("/localityByRegion")
    public Collection<RegionDataLocality> admin() {
        return localityService.getRegionDataLocalities();
    }

}
