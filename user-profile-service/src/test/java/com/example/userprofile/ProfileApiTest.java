package com.example.userprofile;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.jayway.jsonpath.JsonPath;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ProfileApiTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private UserProfileRepository repository;

	@Test
	void registerReadAndUpdateProfile() throws Exception {
		mockMvc.perform(post("/api/users/register")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"account":"bad","password":"secret123","display_name":"错","age":28,"gender":"未知","height_cm":175,"weight_kg":72}
								"""))
				.andExpect(status().isBadRequest());

		MvcResult created = mockMvc.perform(post("/api/users/register")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"account":"zhangsan_sb","password":"secret123","display_name":"张三","age":28,"gender":"男","height_cm":175,"weight_kg":72,"waist_cm":82,"activity_level":"轻度","goal_type":"减重","target_weight_kg":68}
								"""))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.bmi").value(23.51))
				.andExpect(jsonPath("$.bmr").value(1678.75))
				.andExpect(jsonPath("$.tdee_kcal").value(2308.28))
				.andExpect(jsonPath("$.activity_level").value("light"))
				.andExpect(jsonPath("$.goal_type").value("lose"))
				.andExpect(jsonPath("$.source_type").value("real"))
				.andReturn();
		String userId = JsonPath.read(created.getResponse().getContentAsString(), "$.user_id");

		mockMvc.perform(post("/api/users/register")
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"account":"zhangsan_sb","password":"secret123","display_name":"张三","age":28,"gender":"男","height_cm":175,"weight_kg":72}
								"""))
				.andExpect(status().isConflict());

		mockMvc.perform(get("/api/users/" + userId))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.account").value("zhangsan_sb"));

		mockMvc.perform(put("/api/users/" + userId)
						.contentType(MediaType.APPLICATION_JSON)
						.content("""
								{"weight_kg":70,"activity_level":"中度"}
								"""))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.bmi").value(22.86))
				.andExpect(jsonPath("$.pal_coefficient").value(1.55))
				.andExpect(jsonPath("$.tdee_kcal").value(2571.06));

		repository.deleteById(userId);
	}

}
