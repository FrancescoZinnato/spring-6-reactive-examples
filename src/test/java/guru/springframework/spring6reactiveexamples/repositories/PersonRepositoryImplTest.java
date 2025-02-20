package guru.springframework.spring6reactiveexamples.repositories;

import guru.springframework.spring6reactiveexamples.domain.Person;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class PersonRepositoryImplTest {

    PersonRepository repo = new PersonRepositoryImpl();

    @Test
    void testMonoByIdBlock() { // Come NON fare un test con Mono/Flux

        Mono<Person> personMono = repo.getById(1);

        Person person = personMono.block();

        assert person != null;

        System.out.println(person);
    }

    @Test
    void testGetByIdSubscriber() {

        Mono<Person> personMono = repo.getById(1);

        personMono.subscribe(System.out::println);

    }

    @Test
    void testMapOperation() {

        Mono<Person> personMono = repo.getById(1);

        personMono.map(Person::getFirstName).subscribe(System.out::println);

    }

}